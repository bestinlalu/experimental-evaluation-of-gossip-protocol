package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// --- Gossip Structs ---

type NodeState struct {
	ID         string `json:"id"`
	Address    string `json:"address"`
	Data       string `json:"data"`
	Generation int64  `json:"generation"`
	Version    int64  `json:"version"`
	Timestamp  int64  `json:"timestamp"` // Unix epoch in nanoseconds for internal logic
}

// We no longer need Request/Response types, just a standard PUSH payload
type GossipMessage struct {
	Type  string               `json:"type"`  // Will always be "PUSH"
	State map[string]NodeState `json:"state"` // The active node's entire knowledge base
}

// --- Kafka Structs ---

type GossipDigest struct {
	Generation int64  `json:"generation"`
	Version    int64  `json:"version"`
	Timestamp  string `json:"timestamp"` // ISO-8601 string
	IsAlive    bool   `json:"isAlive"`
}

type KafkaEvent struct {
	NodeAddress  string       `json:"nodeAddress"`
	GossipDigest GossipDigest `json:"gossipDigest"`
}

// --- Node Definition ---

type Node struct {
	ID          string
	Address     string
	StateMap    map[string]NodeState
	stateLock   sync.RWMutex
	kafkaWriter *kafka.Writer // Kafka producer
}

func NewNode(id, address string, initialPeers []string, kafkaBroker, kafkaTopic string) *Node {
	n := &Node{
		ID:       id,
		Address:  address,
		StateMap: make(map[string]NodeState),
	}

	// Initialize Kafka Writer if a broker is provided
	if kafkaBroker != "" && kafkaTopic != "" {
		n.kafkaWriter = &kafka.Writer{
			Addr:     kafka.TCP(kafkaBroker),
			Topic:    kafkaTopic,
			Balancer: &kafka.LeastBytes{},
		}
		fmt.Printf("📡 [%s] Kafka producer initialized (Broker: %s, Topic: %s)\n", n.ID, kafkaBroker, kafkaTopic)
	}

	// Initialize own state (Generation acts as the node's boot time)
	n.StateMap[id] = NodeState{
		ID:         id,
		Address:    address,
		Data:       "",
		Generation: time.Now().UnixNano(),
		Version:    0,
		Timestamp:  time.Now().UnixNano(),
	}

	// Record initial peers so we know who to push to
	for _, peerAddr := range initialPeers {
		if peerAddr != "" {
			n.StateMap[peerAddr] = NodeState{Address: peerAddr, Version: -1}
		}
	}

	return n
}

func (n *Node) UpdateOwnData(newData string) {
	n.stateLock.Lock()
	defer n.stateLock.Unlock()

	state := n.StateMap[n.ID]
	state.Data = newData
	state.Version++
	state.Timestamp = time.Now().UnixNano()
	n.StateMap[n.ID] = state

	fmt.Printf("🚀 [%s] Updated own state to: '%s' (v%d)\n", n.ID, newData, state.Version)
}

// StartListening waits for incoming PUSH messages
func (n *Node) StartListening() {
	addr, _ := net.ResolveUDPAddr("udp", n.Address)
	conn, _ := net.ListenUDP("udp", addr)
	defer conn.Close()

	fmt.Printf("🎧 [%s] Listening for gossip on %s...\n", n.ID, n.Address)
	buffer := make([]byte, 8192)

	for {
		length, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			continue
		}

		var msg GossipMessage
		if err := json.Unmarshal(buffer[:length], &msg); err != nil {
			continue
		}

		// In a Push protocol, we only expect PUSH messages. 
		// We immediately merge the received state.
		if msg.Type == "PUSH" {
			n.mergeState(msg.State)
		}
	}
}

// mergeState compares incoming states, updates locals, and fires Kafka events
func (n *Node) mergeState(incoming map[string]NodeState) {
	n.stateLock.Lock()
	defer n.stateLock.Unlock()

	var newKafkaEvents []KafkaEvent

	for id, incState := range incoming {
		if id == n.ID {
			continue // Never overwrite our own authoritative state from someone else
		}

		localState, exists := n.StateMap[id]
		isUpdated := false

		if !exists {
			// Passive Peer Discovery
			n.StateMap[id] = incState
			fmt.Printf("🔍 [%s] Discovered NEW peer: %s at %s\n", n.ID, id, incState.Address)
			isUpdated = true
			if _, tempExists := n.StateMap[incState.Address]; tempExists && id != incState.Address {
				delete(n.StateMap, incState.Address)
			}
		} else {
			// Conflict Resolution
			isNewerGeneration := incState.Generation > localState.Generation
			isNewerVersion := (incState.Generation == localState.Generation) && (incState.Version > localState.Version)

			if isNewerGeneration || isNewerVersion {
				n.StateMap[id] = incState
				fmt.Printf("🔄 [%s] Updated state for %s: '%s' (v%d)\n", n.ID, id, incState.Data, incState.Version)
				isUpdated = true
				if _, tempExists := n.StateMap[incState.Address]; tempExists && id != incState.Address {
					delete(n.StateMap, incState.Address)
				}
			}
		}

		// If a node was updated or newly discovered, prepare a Kafka event
		if isUpdated && n.kafkaWriter != nil {
			ts := time.Unix(0, incState.Timestamp).UTC().Format("2006-01-02T15:04:05.000Z")
			
			newKafkaEvents = append(newKafkaEvents, KafkaEvent{
				NodeAddress: incState.Address,
				GossipDigest: GossipDigest{
					Generation: incState.Generation,
					Version:    incState.Version,
					Timestamp:  ts,
					IsAlive:    true,
				},
			})
		}
	}

	// Publish the array of events to Kafka
	if len(newKafkaEvents) > 0 && n.kafkaWriter != nil {
		payload, _ := json.Marshal(newKafkaEvents)
		err := n.kafkaWriter.WriteMessages(context.Background(), kafka.Message{
			Value: payload,
		})
		
		if err != nil {
			fmt.Printf("⚠️ [%s] Failed to write to Kafka: %v\n", n.ID, err)
		} else {
			fmt.Printf("📨 [%s] Published %d update(s) to Kafka topic.\n", n.ID, len(newKafkaEvents))
		}
	}
}

// StartGossiping aggressively PUSHES its state to random peers
func (n *Node) StartGossiping(interval time.Duration) {
	rand.Seed(time.Now().UnixNano())

	for {
		time.Sleep(interval)

		n.stateLock.RLock()
		
		// 1. Package our entire state map into a PUSH message
		msg := GossipMessage{
			Type:  "PUSH",
			State: n.StateMap,
		}
		payload, _ := json.Marshal(msg)

		// 2. Gather peer addresses
		var peerAddrs []string
		for id, state := range n.StateMap {
			if id != n.ID && state.Address != "" {
				peerAddrs = append(peerAddrs, state.Address)
			}
		}
		n.stateLock.RUnlock()

		if len(peerAddrs) == 0 {
			continue
		}

		// 3. Pick up to 2 random peers
		rand.Shuffle(len(peerAddrs), func(i, j int) {
			peerAddrs[i], peerAddrs[j] = peerAddrs[j], peerAddrs[i]
		})
		
		numToSelect := 2
		if len(peerAddrs) < 2 {
			numToSelect = len(peerAddrs)
		}

		// 4. Force the data onto the selected peers
		for _, targetAddr := range peerAddrs[:numToSelect] {
			fmt.Printf("🟢 [%s] Pushing state to %s\n", n.ID, targetAddr)
			n.sendUDP(targetAddr, payload)
		}
	}
}

func (n *Node) sendUDP(targetAddress string, payload []byte) {
	addr, err := net.ResolveUDPAddr("udp", targetAddress)
	if err != nil { return }
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil { return }
	defer conn.Close()
	conn.Write(payload)
}

// go run main.go -id "Node-1" -addr "127.0.0.1:8001" -peers "127.0.0.1:8002" -kafka-broker "localhost:9092" -kafka-topic "gossip"

func main() {
	idFlag := flag.String("id", "Node-1", "Identifier for the node")
	addrFlag := flag.String("addr", "127.0.0.1:8001", "Address for this node to listen on")
	peersFlag := flag.String("peers", "", "Comma-separated list of peer addresses")
	injectFlag := flag.String("inject", "", "Message to inject to start the gossip")
	
	kafkaBrokerFlag := flag.String("kafka-broker", "", "Kafka broker address (e.g., localhost:9092)")
	kafkaTopicFlag := flag.String("kafka-topic", "gossip-events", "Kafka topic to publish to")

	flag.Parse()

	var initialPeers []string
	if *peersFlag != "" {
		initialPeers = strings.Split(*peersFlag, ",")
	}

	node := NewNode(*idFlag, *addrFlag, initialPeers, *kafkaBrokerFlag, *kafkaTopicFlag)

	go node.StartListening()
	go node.StartGossiping(3 * time.Second)

	if *injectFlag != "" {
		time.Sleep(1 * time.Second)
		node.UpdateOwnData(*injectFlag)
	}

	select {}
}