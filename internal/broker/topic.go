// Package broker implements the MQTT topic trie with wildcard matching.
package broker

import (
	"strings"
	"sync"
)

// subscriptionEntry holds a subscriber reference in the topic trie.
type subscriptionEntry struct {
	clientID string
	sub      topicSubscription
}

type topicSubscription struct {
	filter string
	qos    byte
	nodeID string
}

// trieNode represents a node in the topic trie.
type trieNode struct {
	mu       sync.RWMutex
	children map[string]*trieNode
	subs     []subscriptionEntry
	retained *RetainedMessage
}

// RetainedMessage holds a retained message at a topic path.
type RetainedMessage struct {
	Topic   string
	Payload []byte
	QoS     byte
}

// TopicTrie is a concurrent topic tree with wildcard matching.
type TopicTrie struct {
	root *trieNode
}

// NewTopicTrie creates a new empty TopicTrie.
func NewTopicTrie() *TopicTrie {
	return &TopicTrie{root: newTrieNode()}
}

func newTrieNode() *trieNode {
	return &trieNode{
		children: make(map[string]*trieNode),
	}
}

// Subscribe adds a subscription for clientID to filter.
func (t *TopicTrie) Subscribe(clientID, filter string, qos byte) {
	parts := splitTopic(filter)
	t.root.subscribe(parts, subscriptionEntry{
		clientID: clientID,
		sub:      topicSubscription{filter: filter, qos: qos, nodeID: clientID},
	})
}

// Unsubscribe removes clientID's subscription from filter.
func (t *TopicTrie) Unsubscribe(clientID, filter string) {
	parts := splitTopic(filter)
	t.root.unsubscribe(parts, clientID)
}

// UnsubscribeAll removes all subscriptions for clientID.
func (t *TopicTrie) UnsubscribeAll(clientID string) {
	t.root.unsubscribeAll(clientID)
}

// Match returns all subscriptions matching the given topic name.
// Result is a map from clientID to max QoS.
func (t *TopicTrie) Match(topic string) map[string]byte {
	parts := splitTopic(topic)
	result := make(map[string]byte)
	t.root.match(parts, result)
	return result
}

// SetRetained stores a retained message at the given topic.
func (t *TopicTrie) SetRetained(topic string, payload []byte, qos byte) {
	parts := splitTopic(topic)
	node := t.root.getOrCreate(parts)
	node.mu.Lock()
	defer node.mu.Unlock()
	if len(payload) == 0 {
		node.retained = nil
	} else {
		node.retained = &RetainedMessage{Topic: topic, Payload: payload, QoS: qos}
	}
}

// GetRetained returns all retained messages matching the filter.
func (t *TopicTrie) GetRetained(filter string) []*RetainedMessage {
	parts := splitTopic(filter)
	var results []*RetainedMessage
	t.root.getRetained(parts, &results)
	return results
}

func (n *trieNode) subscribe(parts []string, entry subscriptionEntry) {
	if len(parts) == 0 {
		n.mu.Lock()
		defer n.mu.Unlock()
		// Update existing subscription or add new one
		for i, e := range n.subs {
			if e.clientID == entry.clientID {
				n.subs[i] = entry
				return
			}
		}
		n.subs = append(n.subs, entry)
		return
	}

	part := parts[0]
	n.mu.Lock()
	child, ok := n.children[part]
	if !ok {
		child = newTrieNode()
		n.children[part] = child
	}
	n.mu.Unlock()

	child.subscribe(parts[1:], entry)
}

func (n *trieNode) unsubscribe(parts []string, clientID string) {
	if len(parts) == 0 {
		n.mu.Lock()
		defer n.mu.Unlock()
		for i, e := range n.subs {
			if e.clientID == clientID {
				n.subs = append(n.subs[:i], n.subs[i+1:]...)
				return
			}
		}
		return
	}

	part := parts[0]
	n.mu.RLock()
	child, ok := n.children[part]
	n.mu.RUnlock()
	if ok {
		child.unsubscribe(parts[1:], clientID)
	}
}

func (n *trieNode) unsubscribeAll(clientID string) {
	n.mu.Lock()
	subs := n.subs[:0]
	for _, e := range n.subs {
		if e.clientID != clientID {
			subs = append(subs, e)
		}
	}
	n.subs = subs
	children := make([]*trieNode, 0, len(n.children))
	for _, child := range n.children {
		children = append(children, child)
	}
	n.mu.Unlock()

	for _, child := range children {
		child.unsubscribeAll(clientID)
	}
}

func (n *trieNode) match(parts []string, result map[string]byte) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if len(parts) == 0 {
		// Exact match
		for _, e := range n.subs {
			if q, ok := result[e.clientID]; !ok || e.sub.qos > q {
				result[e.clientID] = e.sub.qos
			}
		}
		// "$SYS" topics do not match "#" at root level
		// but we handle "#" at this node
		if hashChild, ok := n.children["#"]; ok {
			hashChild.mu.RLock()
			for _, e := range hashChild.subs {
				if q, ok := result[e.clientID]; !ok || e.sub.qos > q {
					result[e.clientID] = e.sub.qos
				}
			}
			hashChild.mu.RUnlock()
		}
		return
	}

	part := parts[0]

	// Exact match child
	if child, ok := n.children[part]; ok {
		child.match(parts[1:], result)
	}

	// Single-level wildcard (+)
	if child, ok := n.children["+"]; ok {
		child.match(parts[1:], result)
	}

	// Multi-level wildcard (#) matches everything from here
	if child, ok := n.children["#"]; ok {
		child.mu.RLock()
		for _, e := range child.subs {
			if q, ok := result[e.clientID]; !ok || e.sub.qos > q {
				result[e.clientID] = e.sub.qos
			}
		}
		child.mu.RUnlock()
	}
}

func (n *trieNode) getOrCreate(parts []string) *trieNode {
	if len(parts) == 0 {
		return n
	}
	part := parts[0]
	n.mu.Lock()
	child, ok := n.children[part]
	if !ok {
		child = newTrieNode()
		n.children[part] = child
	}
	n.mu.Unlock()
	return child.getOrCreate(parts[1:])
}

func (n *trieNode) getRetained(parts []string, results *[]*RetainedMessage) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if len(parts) == 0 {
		if n.retained != nil {
			*results = append(*results, n.retained)
		}
		return
	}

	part := parts[0]

	if part == "#" {
		// Collect all retained messages in subtree
		n.collectRetained(results)
		return
	}

	if part == "+" {
		// Match any single level
		for _, child := range n.children {
			child.getRetained(parts[1:], results)
		}
		return
	}

	if child, ok := n.children[part]; ok {
		child.getRetained(parts[1:], results)
	}
}

func (n *trieNode) collectRetained(results *[]*RetainedMessage) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.retained != nil {
		*results = append(*results, n.retained)
	}
	for _, child := range n.children {
		child.collectRetained(results)
	}
}

// splitTopic splits a topic string into its path segments.
func splitTopic(topic string) []string {
	if topic == "" {
		return []string{""}
	}
	return strings.Split(topic, "/")
}

// ValidateTopicName validates an MQTT topic name (not a filter).
func ValidateTopicName(topic string) bool {
	if topic == "" {
		return false
	}
	// Topic names must not contain wildcards
	return !strings.ContainsAny(topic, "+#")
}

// ValidateTopicFilter validates an MQTT topic filter.
func ValidateTopicFilter(filter string) bool {
	if filter == "" {
		return false
	}
	parts := strings.Split(filter, "/")
	for i, part := range parts {
		if part == "#" && i != len(parts)-1 {
			return false // # must be last
		}
		if strings.Contains(part, "+") && part != "+" {
			return false // + must be whole segment
		}
		if strings.Contains(part, "#") && part != "#" {
			return false // # must be whole segment
		}
	}
	return true
}
