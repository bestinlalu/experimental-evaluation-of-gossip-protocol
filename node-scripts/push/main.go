package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// --- Helper function for UID Generation ---
func generateUID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

// --- Gossip Structs ---

type NodeState struct {
	ID         string `json:"id"`
	UID        string `json:"uid"`
	Address    string `json:"address"`
	Data       string `json:"data"`
	Generation int64  `json:"generation"`
	Version    int64  `json:"version"`
	Timestamp  int64  `json:"timestamp"`
	ExpiresAt  int64  `json:"expiresAt"`
}

type GossipMessage struct {
	Type          string               `json:"type"`
	SenderAddress string               `json:"senderAddress"` // NEW: Address of the node sending the UDP packet
	State         map[string]NodeState `json:"state"`
}

// --- Kafka Structs ---

type GossipDigest struct {
	UID                string `json:"uid"`
	Generation         int64  `json:"generation"`
	Version            int64  `json:"version"`
	ForwarderTimestamp string `json:"forwarderTimestamp"`
	CreationTimestamp  string `json:"creationTimestamp"`
	Data               string `json:"data"`
	TTL                int64  `json:"ttl"`
}

type KafkaEvent struct {
	CreatorAddress   string       `json:"creatorAddress"`   // The Creator's Address (Node A)
	ForwarderAddress string       `json:"forwarderAddress"` // The Forwarder's Address (Node B)
	Strategy         string       `json:"strategy"`
	GossipDigest     GossipDigest `json:"gossipDigest"`
}

// --- Node Definition ---

type Node struct {
	ID          string
	Address     string
	StateMap    map[string]NodeState
	stateLock   sync.RWMutex
	kafkaWriter *kafka.Writer
	TTL         time.Duration
}

func NewNode(id, address string, generation int64, ttl time.Duration, initialPeers []string, kafkaBroker, kafkaTopic string) *Node {
	n := &Node{
		ID:       id,
		Address:  address,
		TTL:      ttl,
		StateMap: make(map[string]NodeState),
	}

	if kafkaBroker != "" && kafkaTopic != "" {
		n.kafkaWriter = &kafka.Writer{
			Addr:     kafka.TCP(kafkaBroker),
			Topic:    kafkaTopic,
			Balancer: &kafka.LeastBytes{},
		}
		fmt.Printf("📡 [%s] Kafka producer initialized (Broker: %s, Topic: %s)\n", n.ID, kafkaBroker, kafkaTopic)
	}

	now := time.Now()

	n.StateMap[id] = NodeState{
		ID:         id,
		UID:        generateUID(),
		Address:    address,
		Data:       "",
		Generation: generation,
		Version:    0,
		Timestamp:  now.UnixNano(),
		ExpiresAt:  now.Add(ttl).UnixNano(),
	}

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
	now := time.Now()

	state.Data = newData
	state.Version++
	state.UID = generateUID()
	state.Timestamp = now.UnixNano()
	state.ExpiresAt = now.Add(n.TTL).UnixNano()

	n.StateMap[n.ID] = state

	// Comment this print statement out later if the heartbeat console logs get too noisy!
	fmt.Printf("💓 [%s] Heartbeat/Update: '%s' (v%d) [UID: %s]\n", n.ID, newData, state.Version, state.UID)
}

func (n *Node) StartListening() {
	addr, err := net.ResolveUDPAddr("udp", n.Address)
	if err != nil {
		fmt.Printf("❌ [%s] Failed to resolve listen address: %v\n", n.ID, err)
		return
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		fmt.Printf("❌ [%s] Failed to bind UDP listener: %v\n", n.ID, err)
		return
	}
	defer conn.Close()

	fmt.Printf("🎧 [%s] Listening for gossip on %s...\n", n.ID, n.Address)

	buffer := make([]byte, 65535)

	for {
		// FIXED: Replaced unused senderAddr with blank identifier '_'
		length, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			continue
		}

		var msg GossipMessage
		if err := json.Unmarshal(buffer[:length], &msg); err != nil {
			continue
		}

		if msg.Type == "PUSH" {
			n.mergeState(msg.State)
		}
	}
}

func (n *Node) mergeState(incoming map[string]NodeState) {
	n.stateLock.Lock()
	defer n.stateLock.Unlock()

	var newKafkaEvents []KafkaEvent
	now := time.Now().UnixNano()

	for id, incState := range incoming {
		if id == n.ID {
			continue
		}

		// Prevent accepting records that are already expired
		if incState.ExpiresAt > 0 && now > incState.ExpiresAt {
			continue
		}

		localState, exists := n.StateMap[id]
		isUpdated := false

		if !exists {
			n.StateMap[id] = incState
			fmt.Printf("🔍 [%s] Discovered NEW peer ID: %s (UID: %s)\n", n.ID, id, incState.UID)
			isUpdated = true

			if _, tempExists := n.StateMap[incState.Address]; tempExists && id != incState.Address {
				delete(n.StateMap, incState.Address)
			}
		} else {
			isNewerGeneration := incState.Generation > localState.Generation
			isNewerVersion := (incState.Generation == localState.Generation) && (incState.Version > localState.Version)

			if isNewerGeneration || isNewerVersion {
				n.StateMap[id] = incState
				fmt.Printf("🔄 [%s] Updated state for %s: '%s' (v%d) [UID: %s]\n", n.ID, id, incState.Data, incState.Version, incState.UID)
				isUpdated = true

				if _, tempExists := n.StateMap[incState.Address]; tempExists && id != incState.Address {
					delete(n.StateMap, incState.Address)
				}
			}
		}

		if isUpdated && n.kafkaWriter != nil {
			ts := time.Unix(0, incState.Timestamp).UTC().Format("2006-01-02T15:04:05.000Z")
			newKafkaEvents = append(newKafkaEvents, KafkaEvent{
				CreatorAddress:   incState.Address,
				ForwarderAddress: n.Address,
				Strategy:         "PUSH",
				GossipDigest: GossipDigest{
					UID:                incState.UID,
					Generation:         incState.Generation,
					Version:            incState.Version,
					CreationTimestamp:  ts,
					ForwarderTimestamp: time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
					Data:               incState.Data,
					TTL:                (incState.ExpiresAt - incState.Timestamp) / int64(time.Second),
				},
			})
		}
	}

	if len(newKafkaEvents) > 0 && n.kafkaWriter != nil {
		go func(events []KafkaEvent) {
			payload, err := json.Marshal(events)
			if err != nil {
				fmt.Printf("❌ Failed to marshal Kafka events: %v\n", err)
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// 👉 STRICT ERROR CHECKING HERE
			err = n.kafkaWriter.WriteMessages(ctx, kafka.Message{Value: payload})
			if err != nil {
				fmt.Printf("❌ Kafka Write Error: %v\n", err)
			} else {
				fmt.Printf("✅ Successfully sent %d observed states to Kafka\n", len(events))
			}
		}(newKafkaEvents)
	}
}

func (n *Node) StartGossiping(interval time.Duration) {
	for {
		time.Sleep(interval)

		// 1. Act as a Heartbeat: Refresh own TTL, bump Version, generate new UID
		n.stateLock.RLock()
		currentData := n.StateMap[n.ID].Data
		n.stateLock.RUnlock()

		if currentData == "" {
			currentData = "Alive"
		}
		n.UpdateOwnData(currentData)

		// 2. Prepare payload and Purge expired nodes
		n.stateLock.Lock()

		now := time.Now().UnixNano()
		cleanStateToSend := make(map[string]NodeState)

		for k, v := range n.StateMap {
			if k != n.ID && v.ExpiresAt > 0 && now > v.ExpiresAt {
				fmt.Printf("🗑️ [%s] Record for %s expired. Removing from gossip list.\n", n.ID, k)
				delete(n.StateMap, k)
				continue
			}

			if v.Version >= 0 && v.ID != "" {
				cleanStateToSend[k] = v
			}
		}

		msg := GossipMessage{
			Type:          "PUSH",
			SenderAddress: n.Address, // NEW: Set the Forwarder (Node B) as the sender of this gossip message
			State:         cleanStateToSend,
		}
		payload, _ := json.Marshal(msg)

		var peerAddrs []string
		for id, state := range n.StateMap {
			if id != n.ID && state.Address != "" {
				peerAddrs = append(peerAddrs, state.Address)
			}
		}
		n.stateLock.Unlock()

		if len(peerAddrs) == 0 {
			continue
		}

		numToSelect := 2
		if len(peerAddrs) < 2 {
			numToSelect = len(peerAddrs)
		}

		for _, targetAddr := range peerAddrs[:numToSelect] {
			fmt.Printf("[%s] peers are: %v", n.ID, peerAddrs)
			fmt.Printf("🟢 [%s] Pushing %d states to %s\n", n.ID, len(cleanStateToSend), targetAddr)
			n.sendUDP(targetAddr, payload)
		}
	}
}

func (n *Node) sendUDP(targetAddress string, payload []byte) {
	addr, err := net.ResolveUDPAddr("udp", targetAddress)
	if err != nil {
		return
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return
	}
	defer conn.Close()
	conn.Write(payload)
}

func main() {
	idFlag := flag.String("id", "Node-1", "Identifier for the node")
	addrFlag := flag.String("addr", "127.0.0.1:8001", "Address for this node to listen on")
	peersFlag := flag.String("peers", "", "Comma-separated list of peer addresses")
	injectFlag := flag.String("inject", "", "Message to inject to start the gossip")

	genFlag := flag.Int64("gen", 1, "Generation number for the node (default: 1)")
	ttlFlag := flag.Duration("ttl", 5*time.Second, "Time to live for gossip records (e.g., 30s, 1m)")

	kafkaBrokerFlag := flag.String("kafka-broker", "", "Kafka broker address (e.g., localhost:9092)")
	kafkaTopicFlag := flag.String("kafka-topic", "gossip-events", "Kafka topic to publish to")

	flag.Parse()

	var initialPeers []string
	if *peersFlag != "" {
		initialPeers = strings.Split(*peersFlag, ",")
	}

	node := NewNode(*idFlag, *addrFlag, *genFlag, *ttlFlag, initialPeers, *kafkaBrokerFlag, *kafkaTopicFlag)

	go node.StartListening()
	go node.StartGossiping(3 * time.Second)

	if *injectFlag != "" {
		time.Sleep(1 * time.Second)
		node.UpdateOwnData(*injectFlag)
	}

	// Keep the main thread alive
	select {}
}
