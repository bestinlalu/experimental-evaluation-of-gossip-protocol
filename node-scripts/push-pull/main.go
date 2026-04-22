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

// ---------- Helpers ----------

func generateUID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("fallback-%d", time.Now().UnixNano())
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func getCurrentTimestamp() string {
	return time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
}

func nowUnixNano() int64 {
	return time.Now().UnixNano()
}

func formatUnixNano(ts int64) string {
	return time.Unix(0, ts).UTC().Format("2006-01-02T15:04:05.000Z")
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

// ---------- Gossip Structs ----------

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
	SenderID      string               `json:"senderId,omitempty"`
	State         map[string]NodeState `json:"state,omitempty"`
}

// ---------- Kafka Structs ----------

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

// ---------- Node ----------

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
			Addr:         kafka.TCP(kafkaBroker),
			Topic:        kafkaTopic,
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: kafka.RequireOne,
			Async:        true,
		}
		fmt.Printf("[%s] 📡 [%s] Kafka producer initialized (Broker: %s, Topic: %s)\n",
			getCurrentTimestamp(), n.ID, kafkaBroker, kafkaTopic)
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
		peerAddr = strings.TrimSpace(peerAddr)
		if peerAddr == "" || peerAddr == address {
			continue
		}
		n.StateMap[peerAddr] = NodeState{
			ID:      "",
			Address: peerAddr,
			Version: -1,
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
	state.Address = n.Address
	state.ID = n.ID

	n.StateMap[n.ID] = state

	fmt.Printf("[%s] 💓 [%s] Executing Round %d: '%s' [UID: %s]\n",
		getCurrentTimestamp(), n.ID, state.Version, newData, state.UID)
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
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		length, remoteAddr, err := conn.ReadFromUDP(buffer)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			continue
		}

		var msg GossipMessage
		if err := json.Unmarshal(buffer[:length], &msg); err != nil {
			continue
		}

		switch msg.Type {
		case "PUSH":
			n.mergeState(msg.State)
		case "PULL_REQUEST":
			n.handlePullRequest(conn, remoteAddr, msg)
		case "PULL_RESPONSE":
			n.mergeState(msg.State)
		}
	}
}

func (n *Node) handlePullRequest(conn *net.UDPConn, remoteAddr *net.UDPAddr, msg GossipMessage) {
	if msg.SenderAddress != "" && msg.SenderAddress != n.Address {
		n.stateLock.Lock()
		if _, exists := n.StateMap[msg.SenderAddress]; !exists {
			n.StateMap[msg.SenderAddress] = NodeState{
				ID:      msg.SenderID,
				Address: msg.SenderAddress,
				Version: -1,
			}
		}
		n.stateLock.Unlock()
	}

	snapshot := n.getCleanStateSnapshot()

	resp := GossipMessage{
		Type:          "PULL_RESPONSE",
		SenderAddress: n.Address,
		SenderID:      n.ID,
		State:         snapshot,
	}

	payload, err := json.Marshal(resp)
	if err != nil {
		return
	}

	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.WriteToUDP(payload, remoteAddr); err != nil {
		fmt.Printf("[%s] ❌ [%s] Failed to respond to pull request from %s: %v\n",
			getCurrentTimestamp(), n.ID, remoteAddr.String(), err)
	}
}

func (n *Node) getCleanStateSnapshot() map[string]NodeState {
	n.stateLock.Lock()
	defer n.stateLock.Unlock()

	now := nowUnixNano()
	clean := make(map[string]NodeState)

	for key, st := range n.StateMap {
		if key != n.ID && st.ExpiresAt > 0 && now > st.ExpiresAt {
			fmt.Printf("[%s] 🗑️ [%s] Record for %s expired. Removing from state map.\n",
				getCurrentTimestamp(), n.ID, key)
			delete(n.StateMap, key)
			continue
		}

		if st.Address != "" {
			clean[key] = st
		}
	}

	return clean
}

func isIncomingNewer(localState, incState NodeState, exists bool) bool {
	if !exists {
		return true
	}
	if incState.Generation > localState.Generation {
		return true
	}
	if incState.Generation == localState.Generation && incState.Version > localState.Version {
		return true
	}
	if incState.Generation == localState.Generation &&
		incState.Version == localState.Version &&
		incState.Timestamp > localState.Timestamp {
		return true
	}
	return false
}

func (n *Node) mergeState(incoming map[string]NodeState) {
	if len(incoming) == 0 {
		return
	}

	n.stateLock.Lock()
	defer n.stateLock.Unlock()

	var newKafkaEvents []KafkaEvent
	now := nowUnixNano()

	for incomingKey, incState := range incoming {
		if incState.Address == "" && incomingKey == "" {
			continue
		}

		mapKey := incState.ID
		if mapKey == "" {
			mapKey = incomingKey
		}
		if mapKey == "" {
			mapKey = incState.Address
		}
		if mapKey == "" || mapKey == n.ID || incState.Address == n.Address {
			continue
		}

		if incState.ExpiresAt > 0 && now > incState.ExpiresAt {
			continue
		}

		localState, exists := n.StateMap[mapKey]
		updated := false

		if isIncomingNewer(localState, incState, exists) {
			n.StateMap[mapKey] = incState
			updated = true

			if exists {
				fmt.Printf("[%s] 🔄 [%s] Updated state for %s: '%s' (v%d) [UID: %s]\n",
					getCurrentTimestamp(), n.ID, mapKey, incState.Data, incState.Version, incState.UID)
			} else {
				fmt.Printf("[%s] 🔍 [%s] Discovered NEW peer ID: %s (UID: %s)\n",
					getCurrentTimestamp(), n.ID, mapKey, incState.UID)
			}

			if incState.ID != "" && incState.Address != "" && incState.ID != incState.Address {
				if _, tempExists := n.StateMap[incState.Address]; tempExists {
					delete(n.StateMap, incState.Address)
					fmt.Printf("[%s] 🧹 [%s] Removed stale address entry for %s due to new info from %s\n",
						getCurrentTimestamp(), n.ID, incState.Address, incState.ID)
				}
			}
		}

		if updated && n.kafkaWriter != nil && incState.UID != "" {
			ttlSeconds := int64(0)
			if incState.ExpiresAt > incState.Timestamp {
				ttlSeconds = (incState.ExpiresAt - incState.Timestamp) / int64(time.Second)
			}

			newKafkaEvents = append(newKafkaEvents, KafkaEvent{
				CreatorAddress:   incState.Address,
				ForwarderAddress: n.Address,
				Strategy:         "PUSHPULL",
				GossipDigest: GossipDigest{
					UID:                incState.UID,
					Generation:         incState.Generation,
					Version:            incState.Version,
					CreationTimestamp:  formatUnixNano(incState.Timestamp),
					ForwarderTimestamp: getCurrentTimestamp(),
					Data:               incState.Data,
					TTL:                ttlSeconds,
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

			if err := n.kafkaWriter.WriteMessages(ctx, kafka.Message{Value: payload}); err != nil {
				fmt.Printf("[%s] ❌ [%s] Kafka Write Error: %v\n", getCurrentTimestamp(), n.ID, err)
			}
		}(newKafkaEvents)
	}
}

func (n *Node) evaluateRoundProgress() {
	n.stateLock.RLock()
	myState := n.StateMap[n.ID]
	myVersion := myState.Version
	currentData := myState.Data

	canAdvance := true
	var laggingPeers []string

	for id, state := range n.StateMap {
		if id == n.ID {
			continue
		}
		if state.Version < 0 {
			canAdvance = false
			laggingPeers = append(laggingPeers, id)
			continue
		}
		if state.Version < myVersion {
			canAdvance = false
			laggingPeers = append(laggingPeers, id)
		}
	}
	n.stateLock.RUnlock()

	if canAdvance {
		if currentData == "" || strings.HasPrefix(currentData, "Round") {
			currentData = fmt.Sprintf("Round %d Payload", myVersion+1)
		}
		n.UpdateOwnData(currentData)
		fmt.Printf("[%s] 🚀 [%s] Math confirmation reached! Advanced to Round %d\n",
			getCurrentTimestamp(), n.ID, myVersion+1)
	} else {
		fmt.Printf("[%s] ⏳ [%s] Blocked at Round %d. Waiting on peers: %v\n",
			getCurrentTimestamp(), n.ID, myVersion, laggingPeers)
	}
}

func (n *Node) getPeerAddresses() []string {
	n.stateLock.Lock()
	defer n.stateLock.Unlock()

	now := nowUnixNano()
	seen := make(map[string]bool)
	var peers []string

	for key, st := range n.StateMap {
		if key != n.ID && st.ExpiresAt > 0 && now > st.ExpiresAt {
			fmt.Printf("[%s] 🗑️ [%s] Record for %s expired. Removing from state map.\n",
				getCurrentTimestamp(), n.ID, key)
			delete(n.StateMap, key)
			continue
		}

		if key == n.ID || st.Address == "" || st.Address == n.Address {
			continue
		}

		if !seen[st.Address] {
			seen[st.Address] = true
			peers = append(peers, st.Address)
		}
	}

	return peers
}

func (n *Node) StartGossiping(interval time.Duration, fanout int) {
	for {
		time.Sleep(interval)

		n.evaluateRoundProgress()

		peers := n.getPeerAddresses()
		if len(peers) == 0 {
			continue
		}

		selected := randomSample(peers, fanout)

		// PUSH payload
		pushMsg := GossipMessage{
			Type:          "PUSH",
			SenderAddress: n.Address,
			SenderID:      n.ID,
			State:         n.getCleanStateSnapshot(),
		}
		pushPayload, err := json.Marshal(pushMsg)
		if err != nil {
			continue
		}

		// PULL request payload
		pullReq := GossipMessage{
			Type:          "PULL_REQUEST",
			SenderAddress: n.Address,
			SenderID:      n.ID,
		}
		pullPayload, err := json.Marshal(pullReq)
		if err != nil {
			continue
		}

		for _, targetAddr := range selected {
			n.sendUDP(targetAddr, pushPayload)
			n.sendUDP(targetAddr, pullPayload)
		}
	}
}

func (n *Node) sendUDP(targetAddress string, payload []byte) {
	addr, err := net.ResolveUDPAddr("udp", targetAddress)
	if err != nil {
		fmt.Printf("[%s] ❌ [%s] Resolution error for peer %s: %v\n",
			getCurrentTimestamp(), n.ID, targetAddress, err)
		return
	}

	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		fmt.Printf("[%s] ❌ [%s] Dial error to peer %s: %v\n",
			getCurrentTimestamp(), n.ID, targetAddress, err)
		return
	}
	defer conn.Close()

	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))

	if _, err := conn.Write(payload); err != nil {
		fmt.Printf("[%s] ❌ [%s] Failed to send message to %s: %v\n",
			getCurrentTimestamp(), n.ID, targetAddress, err)
	}
}

func main() {
	idFlag := flag.String("id", "Node-1", "Identifier for the node")
	addrFlag := flag.String("addr", "127.0.0.1:8001", "Address for this node to listen on")
	peersFlag := flag.String("peers", "", "Comma-separated list of peer addresses")
	injectFlag := flag.String("inject", "", "Message to inject to start the gossip")

	genFlag := flag.Int64("gen", 1, "Generation number for the node")
	ttlFlag := flag.Duration("ttl", 5*time.Second, "Time to live for gossip records")
	intervalFlag := flag.Duration("interval", 1*time.Second, "Gossip interval")
	fanoutFlag := flag.Int("fanout", 2, "Number of peers to gossip with each round")

	kafkaBrokerFlag := flag.String("kafka-broker", "", "Kafka broker address")
	kafkaTopicFlag := flag.String("kafka-topic", "gossip-events", "Kafka topic to publish to")

	flag.Parse()

	var initialPeers []string
	if *peersFlag != "" {
		initialPeers = strings.Split(*peersFlag, ",")
	}

	node := NewNode(
		*idFlag,
		*addrFlag,
		*genFlag,
		*ttlFlag,
		initialPeers,
		*kafkaBrokerFlag,
		*kafkaTopicFlag,
	)

	go node.StartListening()
	go node.StartGossiping(*intervalFlag, *fanoutFlag)

	if *injectFlag != "" {
		time.Sleep(1 * time.Second)
		node.UpdateOwnData(*injectFlag)
	}

	select {}
}