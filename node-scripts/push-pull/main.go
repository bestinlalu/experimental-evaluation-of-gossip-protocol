package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"math/big"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// --- Structs ---

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
	SenderID      string               `json:"senderId"`
	SenderAddress string               `json:"senderAddress"`
	State         map[string]NodeState `json:"state"`
}

type KafkaEvent struct {
	CreatorAddress   string       `json:"creatorAddress"`
	ForwarderAddress string       `json:"forwarderAddress"`
	Strategy         string       `json:"strategy"`
	GossipDigest     GossipDigest `json:"gossipDigest"`
}

type GossipDigest struct {
	UID                string   `json:"uid"`
	Generation         int64    `json:"generation"`
	Version            int64    `json:"version"`
	ForwarderTimestamp string   `json:"forwarderTimestamp"`
	CreationTimestamp  string   `json:"creationTimestamp"`
	Data               string   `json:"data"`
	TTL                int64    `json:"ttl"`
	Neighbors          []string `json:"neighbors"`
}

// --- Helper functions ---

func generateUID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
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
		Generation: generation,
		Version:    0,
		Timestamp:  now.UnixNano(),
		ExpiresAt:  now.Add(ttl).UnixNano(),
	}

	for _, peerAddr := range initialPeers {
		peerAddr = strings.TrimSpace(peerAddr)
		if peerAddr != "" && peerAddr != address {
			n.StateMap[peerAddr] = NodeState{Address: peerAddr, Version: -1}
		}
	}

	return n
}

func (n *Node) StartListening() {
	addr, _ := net.ResolveUDPAddr("udp", n.Address)
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		fmt.Printf("❌ Failed to bind UDP: %v\n", err)
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

		n.mergeState(msg)

		if msg.Type == "PUSH_PULL" {
			n.sendPullResponse(msg.SenderAddress)
		}
	}
}

func (n *Node) sendPullResponse(target string) {
	n.stateLock.RLock()
	cleanState := n.getCleanStateMap()
	n.stateLock.RUnlock()

	msg := GossipMessage{
		Type:          "PULL_RESPONSE",
		SenderID:      n.ID,
		SenderAddress: n.Address,
		State:         cleanState,
	}
	payload, _ := json.Marshal(msg)
	n.sendUDP(target, payload)
}

func (n *Node) getCleanStateMap() map[string]NodeState {
	now := time.Now().UnixNano()
	clean := make(map[string]NodeState)
	for k, v := range n.StateMap {
		if k != n.ID && v.ExpiresAt > 0 && now > v.ExpiresAt {
			continue
		}
		clean[k] = v
	}
	return clean
}

func (n *Node) mergeState(msg GossipMessage) {
	n.stateLock.Lock()
	defer n.stateLock.Unlock()

	now := time.Now().UnixNano()
	var newKafkaEvents []KafkaEvent

	for id, incState := range msg.State {
		if id == n.ID || (incState.ExpiresAt > 0 && now > incState.ExpiresAt) {
			continue
		}

		existingKey := ""
		for k, v := range n.StateMap {
			if k == id || (v.Address == incState.Address && v.Address != "") {
				existingKey = k
				break
			}
		}

		isUpdated := false
		if existingKey == "" {
			n.StateMap[id] = incState
			fmt.Printf("[%s] 🔍 [%s] Discovered NEW peer ID: %s (UID: %s)\n", getCurrentTimestamp(), n.ID, id, incState.UID)
			isUpdated = true
		} else {
			localState := n.StateMap[existingKey]

			// Identity Migration check (IP placeholder -> Node ID)
			if existingKey != id && !strings.Contains(id, ":") {
				fmt.Printf("[%s] 🧹 [%s] Removed stale address entry for %s due to new info from %s\n", getCurrentTimestamp(), n.ID, existingKey, id)
				delete(n.StateMap, existingKey)
				n.StateMap[id] = incState
				isUpdated = true
			} else if incState.Generation > localState.Generation || (incState.Generation == localState.Generation && incState.Version > localState.Version) {
				fmt.Printf("[%s] 🔄 [%s] Updated state for %s: '%s' (v%d) [UID: %s]\n", getCurrentTimestamp(), n.ID, id, incState.Data, incState.Version, incState.UID)
				n.StateMap[id] = incState
				isUpdated = true
			}
		}

		if isUpdated && n.kafkaWriter != nil && incState.ID != "" {
			// Get current neighbors for Kafka
			currentNeighbors := []string{}
			for _, s := range n.StateMap {
				if s.Address != "" && s.Address != n.Address {
					currentNeighbors = append(currentNeighbors, s.Address)
				}
			}

			newKafkaEvents = append(newKafkaEvents, KafkaEvent{
				CreatorAddress:   incState.Address,
				ForwarderAddress: n.Address,
				Strategy:         "PUSH_PULL",
				GossipDigest: GossipDigest{
					UID:                incState.UID,
					Generation:         incState.Generation,
					Version:            incState.Version,
					CreationTimestamp:  time.Unix(0, incState.Timestamp).UTC().Format("2006-01-02T15:04:05.000Z"),
					ForwarderTimestamp: getCurrentTimestamp(),
					Data:               incState.Data,
					TTL:                int64(n.TTL.Seconds()),
					Neighbors:          currentNeighbors,
				},
			})
		}
	}

	if len(newKafkaEvents) > 0 {
		go func(events []KafkaEvent) {
			payload, _ := json.Marshal(events)
			n.kafkaWriter.WriteMessages(context.Background(), kafka.Message{Value: payload})
		}(newKafkaEvents)
	}
}

func (n *Node) StartGossiping(interval time.Duration, fanout int) {
	for {
		time.Sleep(interval)

		n.stateLock.Lock()
		myState := n.StateMap[n.ID]
		myState.Timestamp = time.Now().UnixNano()
		myState.ExpiresAt = time.Now().Add(n.TTL).UnixNano()
		n.StateMap[n.ID] = myState

		canAdvance := true
		var lagging []string
		for id, state := range n.StateMap {
			if id == n.ID {
				continue
			}
			if state.Version < myState.Version {
				canAdvance = false
				// If ID is empty, it's an address placeholder
				if state.ID != "" {
					lagging = append(lagging, state.ID)
				} else {
					lagging = append(lagging, state.Address)
				}
			}
		}

		if len(n.StateMap) > 1 && canAdvance {
			myState.Version++
			myState.UID = generateUID()
			myState.Data = fmt.Sprintf("Round %d Payload", myState.Version)
			n.StateMap[n.ID] = myState
			fmt.Printf("[%s] 💓 [%s] Executing Round %d: '%s' [UID: %s]\n", getCurrentTimestamp(), n.ID, myState.Version, myState.Data, myState.UID)
			fmt.Printf("[%s] 🚀 [%s] Math confirmation reached! Advanced to Round %d\n", getCurrentTimestamp(), n.ID, myState.Version)
		} else if len(n.StateMap) > 1 {
			fmt.Printf("[%s] ⏳ [%s] Blocked at Round %d. Waiting on peers: %v\n", getCurrentTimestamp(), n.ID, myState.Version, lagging)
		}

		cleanState := n.getCleanStateMap()
		var peers []string
		for id, s := range n.StateMap {
			if id != n.ID && s.Address != "" {
				peers = append(peers, s.Address)
			}
		}
		n.stateLock.Unlock()

		if len(peers) > 0 {
			msg := GossipMessage{Type: "PUSH_PULL", SenderID: n.ID, SenderAddress: n.Address, State: cleanState}
			payload, _ := json.Marshal(msg)
			for _, target := range randomSample(peers, fanout) {
				n.sendUDP(target, payload)
			}
		}
	}
}

func (n *Node) sendUDP(target string, payload []byte) {
	addr, _ := net.ResolveUDPAddr("udp", target)
	conn, err := net.DialUDP("udp", nil, addr)
	if err == nil {
		defer conn.Close()
		conn.Write(payload)
	}
}

func randomSample(items []string, k int) []string {
	if len(items) <= k {
		return items
	}
	shuffled := make([]string, len(items))
	copy(shuffled, items)
	for i := len(shuffled) - 1; i > 0; i-- {
		jBig, _ := rand.Int(rand.Reader, big.NewInt(int64(i+1)))
		j := int(jBig.Int64())
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	}
	return shuffled[:k]
}

func main() {
	id := flag.String("id", "Node-1", "ID")
	addr := flag.String("addr", "127.0.0.1:8001", "Listen Address")
	peers := flag.String("peers", "", "Initial Peers")
	gen := flag.Int64("gen", 1, "Generation")
	ttl := flag.Duration("ttl", 10*time.Second, "TTL")
	fanout := flag.Int("fanout", 2, "Fanout")
	kafkaB := flag.String("kafka-broker", "", "Kafka Broker")
	kafkaT := flag.String("kafka-topic", "gossip", "Kafka Topic")
	flag.Parse()

	var initialPeers []string
	if *peers != "" {
		initialPeers = strings.Split(*peers, ",")
	}

	node := NewNode(*id, *addr, *gen, *ttl, initialPeers, *kafkaB, *kafkaT)

	go node.StartListening()
	node.StartGossiping(1*time.Second, *fanout)
}
