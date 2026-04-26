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
	UID                string   `json:"uid"`
	Generation         int64    `json:"generation"`
	Version            int64    `json:"version"`
	ForwarderTimestamp string   `json:"forwarderTimestamp"`
	CreationTimestamp  string   `json:"creationTimestamp"`
	Data               string   `json:"data"`
	TTL                int64    `json:"ttl"`
	ActiveNeighbors    []string `json:"activeNeighbors"`
    InactiveNeighbors  []string `json:"inactiveNeighbors"`
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

func formatUnixNano(ts int64) string {
	return time.Unix(0, ts).UTC().Format("2006-01-02T15:04:05.000Z")
}

func nowUnixNano() int64 {
	return time.Now().UnixNano()
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
			RequiredAcks: kafka.RequireOne,
			Async:        true,
			BatchBytes: 10485760,
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

func randomSample(items []string, k int) []string {
	if len(items) <= k {
		return items
	}

	shuffled := make([]string, len(items))
	copy(shuffled, items)

	for i := len(shuffled) - 1; i > 0; i-- {
		jBig, err := rand.Int(rand.Reader, big.NewInt(int64(i+1)))
		if err != nil {
			jBig = big.NewInt(int64(i))
		}
		j := int(jBig.Int64())
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	}

	return shuffled[:k]
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

	// fmt.Printf("[%s] 🎧 [%s] Listening for gossip on %s...\n", getCurrentTimestamp(), n.ID, n.Address)

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

	// --- 1. Snapshot the current neighbor list ---
	activeSet := make(map[string]struct{})
    inactiveSet := make(map[string]struct{})

    for idOrAddr, st := range n.StateMap {
        // Skip self
        if idOrAddr == n.ID || st.ID == n.ID {
            continue
        }

        // Determine the display name (ID preferred, fallback to Address)
        displayName := st.ID
        if displayName == "" {
            displayName = st.Address
        }
        if displayName == "" {
            displayName = idOrAddr
        }

        // Check if the record is active
        if st.Version >= 0 && st.ExpiresAt > now {
            activeSet[displayName] = struct{}{}
        } else {
            inactiveSet[displayName] = struct{}{}
        }
    }

    // Convert sets to slices for JSON (ensures empty list [] instead of null)
    activeNeighbors := []string{}
    for addr := range activeSet {
        activeNeighbors = append(activeNeighbors, addr)
    }

    inactiveNeighbors := []string{}
    for addr := range inactiveSet {
        // Ensure an address doesn't appear in both lists
        if _, active := activeSet[addr]; !active {
            inactiveNeighbors = append(inactiveNeighbors, addr)
        }
    }

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
			// fmt.Printf("[%s] 🔍 [%s] Discovered NEW peer ID: %s (UID: %s)\n", getCurrentTimestamp(), n.ID, id, incState.UID)
			isUpdated = true

			if _, tempExists := n.StateMap[incState.Address]; tempExists && id != incState.Address {
				// fmt.Printf("[%s] 🧹 [%s] Removed stale address entry for %s due to new info from %s\n", getCurrentTimestamp(), n.ID, incState.Address, id)
				delete(n.StateMap, incState.Address)
			}
		} else {
			isNewerGeneration := incState.Generation > localState.Generation
			isNewerVersion := (incState.Generation == localState.Generation) && (incState.Version > localState.Version)

			if isNewerGeneration || isNewerVersion {
				n.StateMap[id] = incState
				// fmt.Printf("[%s] 🔄 [%s] Updated state for %s: '%s' (v%d) [UID: %s]\n", getCurrentTimestamp(), n.ID, id, incState.Data, incState.Version, incState.UID)
				isUpdated = true

				if _, tempExists := n.StateMap[incState.Address]; tempExists && id != incState.Address {
					// fmt.Printf("[%s] 🧹 [%s] Removed stale address entry for %s due to new info from %s\n", getCurrentTimestamp(), n.ID, incState.Address, id)
					delete(n.StateMap, incState.Address)
				}
			}
		}

		if isUpdated && n.kafkaWriter != nil {
			newKafkaEvents = append(newKafkaEvents, KafkaEvent{
				CreatorAddress:   incState.Address,
				ForwarderAddress: n.Address,
				Strategy:         "PUSH",
				GossipDigest: GossipDigest{
					UID:                incState.UID,
					Generation:         incState.Generation,
					Version:            incState.Version,
					CreationTimestamp:  formatUnixNano(incState.Timestamp),
					ForwarderTimestamp: getCurrentTimestamp(),
					Data:               incState.Data,
					TTL:                (incState.ExpiresAt - incState.Timestamp) / int64(time.Second),
					ActiveNeighbors:    activeNeighbors,
                    InactiveNeighbors:  inactiveNeighbors, 
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

func (n *Node) refreshHeartbeat() {
	n.stateLock.Lock()
	defer n.stateLock.Unlock()

	state := n.StateMap[n.ID]
	state.Timestamp = nowUnixNano()
	state.ExpiresAt = time.Now().Add(n.TTL).UnixNano()
	n.StateMap[n.ID] = state
}

func (n *Node) evaluateRoundProgress() {
    n.stateLock.RLock()
    myState := n.StateMap[n.ID]
    myVersion := myState.Version
    currentData := myState.Data

    totalPeers := 0
    updatedPeers := 0
    var laggingPeers []string

    for id, state := range n.StateMap {
        if id == n.ID {
            continue
        }
        
        totalPeers++
        
        // A peer is "updated" if it has a valid version >= our current version
        if state.Version >= myVersion {
            updatedPeers++
        } else {
            laggingPeers = append(laggingPeers, id)
        }
    }
    n.stateLock.RUnlock()

    // Handle the case where we have no peers yet
    canAdvance := false
    if totalPeers > 0 {
        participation := float64(updatedPeers) / float64(totalPeers)
        if participation >= 0.75 {
            canAdvance = true
        }
    }

    if canAdvance {
        if currentData == "" || strings.HasPrefix(currentData, "Round") {
            currentData = fmt.Sprintf("Round %d Payload", myVersion+1)
        }
        n.UpdateOwnData(currentData)
        // fmt.Printf("[%s] 🚀 [%s] 75%% Threshold Met (%d/%d)! Advanced to Round %d\n",
        //     getCurrentTimestamp(), n.ID, updatedPeers, totalPeers, myVersion+1)
    } else {
        // fmt.Printf("[%s] ⏳ [%s] Blocked at Round %d. Progress: %d/%d (Need 75%%). Waiting on: %v\n",
        //     getCurrentTimestamp(), n.ID, myVersion, updatedPeers, totalPeers, laggingPeers)
        n.refreshHeartbeat()
    }
}

func (n *Node) StartGossiping(interval time.Duration, fanout int) {
	for {
		time.Sleep(interval)

		// --- 1. VECTOR CLOCK EVALUATION ---
		n.evaluateRoundProgress()

		// --- 3. GOSSIP CURRENT STATE ---
		n.stateLock.Lock()
		now := time.Now().UnixNano()
		cleanStateToSend := make(map[string]NodeState)

		for k, v := range n.StateMap {
			// Purge expired records so a permanently dead node doesn't block the cluster forever
			if k != n.ID && v.ExpiresAt > 0 && now > v.ExpiresAt {
				// fmt.Printf("[%s] 🗑️ [%s] Record for %s expired. Removing from state map.\n", getCurrentTimestamp(), n.ID, k)
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

		// if len(peerAddrs) < fanout {
		// 	numToSelect = len(peerAddrs)
		// }

		for _, targetAddr := range randomSample(peerAddrs, fanout) {
			n.sendUDP(targetAddr, payload)
		}
	}
}

func (n *Node) sendUDP(targetAddress string, payload []byte) {
	addr, err := net.ResolveUDPAddr("udp", targetAddress)
	if err != nil {
		fmt.Printf("[%s] ❌ [%s] Resolution error for peer %s: %v\n", getCurrentTimestamp(), n.ID, targetAddress, err)
		return
	}

	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		fmt.Printf("[%s] ❌ [%s] Dial error to peer %s: %v\n", getCurrentTimestamp(), n.ID, targetAddress, err)
		return
	}
	defer conn.Close()

	_, err = conn.Write(payload)
	if err != nil {
		fmt.Printf("[%s] ❌ [%s] Failed to SEND gossip to %s: %v\n", getCurrentTimestamp(), n.ID, targetAddress, err)
	} else {
		// Optional: Debugging log for successful sends
		// fmt.Printf("[%s] 📤 [%s] Gossip sent to %s\n", getCurrentTimestamp(), n.ID, targetAddress)
	}
}

func main() {
	idFlag := flag.String("id", "Node-1", "Identifier for the node")
	addrFlag := flag.String("addr", "127.0.0.1:8001", "Address for this node to listen on")
	peersFlag := flag.String("peers", "", "Comma-separated list of peer addresses")
	injectFlag := flag.String("inject", "", "Message to inject to start the gossip")
	fanoutFlag := flag.Int("fanout", 2, "Number of peers to pull from each round")

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
	go node.StartGossiping(1*time.Second, *fanoutFlag)

	if *injectFlag != "" {
		time.Sleep(1 * time.Second)
		node.UpdateOwnData(*injectFlag)
	}

	select {}
}
