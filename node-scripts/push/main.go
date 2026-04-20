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
	SenderAddress string               `json:"senderAddress"`
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
	CreatorAddress   string       `json:"creatorAddress"`
	ForwarderAddress string       `json:"forwarderAddress"`
	Strategy         string       `json:"strategy"`
	GossipDigest     GossipDigest `json:"gossipDigest"`
}

func getCurrentTimestamp() string {
	return time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
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
		fmt.Printf("[%s] 📡 [%s] Kafka producer initialized (Broker: %s, Topic: %s)\n", getCurrentTimestamp(), n.ID, kafkaBroker, kafkaTopic)
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
			// Version -1 ensures we wait for them to report in before advancing past Round 0
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
	state.Version++ // Bumping Version acts as moving to the next Round
	state.UID = generateUID()
	state.Timestamp = now.UnixNano()
	state.ExpiresAt = now.Add(n.TTL).UnixNano()

	n.StateMap[n.ID] = state

	fmt.Printf("[%s] 💓 [%s] Executing Round %d: '%s' [UID: %s]\n", getCurrentTimestamp(), n.ID, state.Version, newData, state.UID)
}

func (n *Node) StartListening() {
	addr, err := net.ResolveUDPAddr("udp", n.Address)
	if err != nil {
		fmt.Printf("[%s] ❌ [%s] Failed to resolve listen address: %v\n", getCurrentTimestamp(), n.ID, err)
		return
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		fmt.Printf("[%s] ❌ [%s] Failed to bind UDP listener: %v\n", getCurrentTimestamp(), n.ID, err)
		return
	}
	defer conn.Close()

	fmt.Printf("[%s] 🎧 [%s] Listening for gossip on %s...\n", getCurrentTimestamp(), n.ID, n.Address)

	buffer := make([]byte, 65535)

	for {
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

		if incState.ExpiresAt > 0 && now > incState.ExpiresAt {
			continue
		}

		localState, exists := n.StateMap[id]
		isUpdated := false

		if !exists {
			n.StateMap[id] = incState
			fmt.Printf("[%s] 🔍 [%s] Discovered NEW peer ID: %s (UID: %s)\n", getCurrentTimestamp(), n.ID, id, incState.UID)
			isUpdated = true

			if _, tempExists := n.StateMap[incState.Address]; tempExists && id != incState.Address {
				fmt.Printf("[%s] 🧹 [%s] Removed stale address entry for %s due to new info from %s\n", getCurrentTimestamp(), n.ID, incState.Address, id)
				delete(n.StateMap, incState.Address)
			}
		} else {
			isNewerGeneration := incState.Generation > localState.Generation
			isNewerVersion := (incState.Generation == localState.Generation) && (incState.Version > localState.Version)

			if isNewerGeneration || isNewerVersion {
				n.StateMap[id] = incState
				fmt.Printf("[%s] 🔄 [%s] Updated state for %s: '%s' (v%d) [UID: %s]\n", getCurrentTimestamp(), n.ID, id, incState.Data, incState.Version, incState.UID)
				isUpdated = true

				if _, tempExists := n.StateMap[incState.Address]; tempExists && id != incState.Address {
					fmt.Printf("[%s] 🧹 [%s] Removed stale address entry for %s due to new info from %s\n", getCurrentTimestamp(), n.ID, incState.Address, id)
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
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			err = n.kafkaWriter.WriteMessages(ctx, kafka.Message{Value: payload})
			if err != nil {
				fmt.Printf("[%s] ❌ [%s] Kafka Write Error: %v\n", getCurrentTimestamp(), n.ID, err)
			}
		}(newKafkaEvents)
	}
}

func (n *Node) StartGossiping(interval time.Duration) {
	for {
		time.Sleep(interval)

		// --- 1. VECTOR CLOCK EVALUATION ---
		n.stateLock.RLock()
		myState := n.StateMap[n.ID]
		myVersion := myState.Version
		currentData := myState.Data

		canAdvance := true
		var laggingPeers []string

		// Check if min(V_new) >= myVersion
		for id, state := range n.StateMap {
			if id == n.ID {
				continue
			}
			if state.Version < myVersion {
				canAdvance = false
				laggingPeers = append(laggingPeers, id)
			}
		}
		n.stateLock.RUnlock()

		// --- 2. ADVANCE ROUND OR WAIT ---
		if canAdvance {
			if currentData == "" || strings.HasPrefix(currentData, "Round") {
				currentData = fmt.Sprintf("Round %d Payload", myVersion+1)
			}
			// This internally increments the Version mathematically confirming the round
			n.UpdateOwnData(currentData)
			fmt.Printf("[%s] 🚀 [%s] Math confirmation reached! Advanced to Round %d\n", getCurrentTimestamp(), n.ID, myVersion+1)
		} else {
			fmt.Printf("[%s] ⏳ [%s] Blocked at Round %d. Waiting on peers: %v\n", getCurrentTimestamp(), n.ID, myVersion, laggingPeers)
		}

		// --- 3. GOSSIP CURRENT STATE ---
		n.stateLock.Lock()
		now := time.Now().UnixNano()
		cleanStateToSend := make(map[string]NodeState)

		for k, v := range n.StateMap {
			// Purge expired records so a permanently dead node doesn't block the cluster forever
			if k != n.ID && v.ExpiresAt > 0 && now > v.ExpiresAt {
				fmt.Printf("[%s] 🗑️ [%s] Record for %s expired. Removing from state map.\n", getCurrentTimestamp(), n.ID, k)
				delete(n.StateMap, k)
				continue
			}

			if v.Version >= 0 && v.ID != "" {
				cleanStateToSend[k] = v
			}
		}

		msg := GossipMessage{
			Type:          "PUSH",
			SenderAddress: n.Address,
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
	ttlFlag := flag.Duration("ttl", 5*time.Second, "Time to live for gossip records")

	kafkaBrokerFlag := flag.String("kafka-broker", "", "Kafka broker address")
	kafkaTopicFlag := flag.String("kafka-topic", "gossip-events", "Kafka topic to publish to")

	flag.Parse()

	var initialPeers []string
	if *peersFlag != "" {
		initialPeers = strings.Split(*peersFlag, ",")
	}

	node := NewNode(*idFlag, *addrFlag, *genFlag, *ttlFlag, initialPeers, *kafkaBrokerFlag, *kafkaTopicFlag)

	go node.StartListening()
	// Ticking every 1 second keeps gossip fast, but it will only advance the round
	// if the vector clock condition is met!
	go node.StartGossiping(1 * time.Second)

	if *injectFlag != "" {
		time.Sleep(1 * time.Second)
		node.UpdateOwnData(*injectFlag)
	}

	select {}
}
