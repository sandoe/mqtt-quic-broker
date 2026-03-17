package broker_test

import (
	"testing"

	"github.com/sandoe/mqtt-quic-broker/internal/broker"
)

func TestTopicTrieExactMatch(t *testing.T) {
	trie := broker.NewTopicTrie()

	trie.Subscribe("client1", "sensors/temp", 0)
	trie.Subscribe("client2", "sensors/temp", 1)

	matches := trie.Match("sensors/temp")
	if len(matches) != 2 {
		t.Fatalf("expected 2 matches, got %d", len(matches))
	}
	if matches["client1"] != 0 {
		t.Errorf("client1 QoS: got %d, want 0", matches["client1"])
	}
	if matches["client2"] != 1 {
		t.Errorf("client2 QoS: got %d, want 1", matches["client2"])
	}
}

func TestTopicTrieNoMatch(t *testing.T) {
	trie := broker.NewTopicTrie()
	trie.Subscribe("client1", "sensors/temp", 0)

	matches := trie.Match("sensors/humidity")
	if len(matches) != 0 {
		t.Errorf("expected 0 matches, got %d", len(matches))
	}
}

func TestTopicTrieSingleLevelWildcard(t *testing.T) {
	trie := broker.NewTopicTrie()

	trie.Subscribe("client1", "sensors/+/temp", 0)

	matches := trie.Match("sensors/room1/temp")
	if _, ok := matches["client1"]; !ok {
		t.Error("client1 should match sensors/+/temp with sensors/room1/temp")
	}

	matches = trie.Match("sensors/room2/temp")
	if _, ok := matches["client1"]; !ok {
		t.Error("client1 should match sensors/+/temp with sensors/room2/temp")
	}

	// Should NOT match two levels
	matches = trie.Match("sensors/room1/sub/temp")
	if _, ok := matches["client1"]; ok {
		t.Error("client1 should NOT match sensors/+/temp with sensors/room1/sub/temp")
	}
}

func TestTopicTrieMultiLevelWildcard(t *testing.T) {
	trie := broker.NewTopicTrie()

	trie.Subscribe("client1", "sensors/#", 1)
	trie.Subscribe("client2", "alarm/#", 2)

	// sensors/# should match all sensors topics
	for _, topic := range []string{
		"sensors/temp",
		"sensors/room1/temp",
		"sensors/room1/floor1/temp",
	} {
		matches := trie.Match(topic)
		if _, ok := matches["client1"]; !ok {
			t.Errorf("client1 should match %q with sensors/#", topic)
		}
		if _, ok := matches["client2"]; ok {
			t.Errorf("client2 should NOT match %q with alarm/#", topic)
		}
	}

	// alarm/# should match
	matches := trie.Match("alarm/critical/zone-1")
	if _, ok := matches["client2"]; !ok {
		t.Error("client2 should match alarm/critical/zone-1 with alarm/#")
	}
}

func TestTopicTrieHashMatchesParentLevel(t *testing.T) {
	trie := broker.NewTopicTrie()
	trie.Subscribe("client1", "sensors/#", 0)

	// "sensors/#" should match "sensors" and "sensors/anything"
	matches := trie.Match("sensors/temp")
	if _, ok := matches["client1"]; !ok {
		t.Error("sensors/# should match sensors/temp")
	}
}

func TestTopicTrieUnsubscribe(t *testing.T) {
	trie := broker.NewTopicTrie()
	trie.Subscribe("client1", "test/topic", 0)
	trie.Subscribe("client2", "test/topic", 1)

	trie.Unsubscribe("client1", "test/topic")

	matches := trie.Match("test/topic")
	if _, ok := matches["client1"]; ok {
		t.Error("client1 should be unsubscribed")
	}
	if _, ok := matches["client2"]; !ok {
		t.Error("client2 should still be subscribed")
	}
}

func TestTopicTrieUnsubscribeAll(t *testing.T) {
	trie := broker.NewTopicTrie()
	trie.Subscribe("client1", "a/b", 0)
	trie.Subscribe("client1", "c/d", 0)
	trie.Subscribe("client2", "a/b", 0)

	trie.UnsubscribeAll("client1")

	if m := trie.Match("a/b"); len(m) != 1 {
		t.Errorf("expected 1 match after unsubscribeAll, got %d", len(m))
	}
	if m := trie.Match("c/d"); len(m) != 0 {
		t.Errorf("expected 0 matches for c/d after unsubscribeAll, got %d", len(m))
	}
}

func TestTopicTrieRetainedMessages(t *testing.T) {
	trie := broker.NewTopicTrie()

	trie.SetRetained("sensors/temp", []byte("22.5"), 0)
	trie.SetRetained("sensors/humidity", []byte("65%"), 1)
	trie.SetRetained("alarm/fire", []byte("active"), 2)

	// Get retained messages matching sensors/#
	retained := trie.GetRetained("sensors/#")
	if len(retained) != 2 {
		t.Errorf("expected 2 retained messages for sensors/#, got %d", len(retained))
	}

	// Get exact match
	retained = trie.GetRetained("alarm/fire")
	if len(retained) != 1 {
		t.Errorf("expected 1 retained message for alarm/fire, got %d", len(retained))
	}
	if string(retained[0].Payload) != "active" {
		t.Errorf("unexpected payload: %q", retained[0].Payload)
	}

	// Delete retained message by publishing empty payload
	trie.SetRetained("sensors/temp", nil, 0)
	retained = trie.GetRetained("sensors/temp")
	if len(retained) != 0 {
		t.Errorf("expected 0 retained messages after deletion, got %d", len(retained))
	}
}

func TestTopicTrieQoSDowngrade(t *testing.T) {
	trie := broker.NewTopicTrie()

	// Client subscribes at QoS1 and QoS2 to same topic
	trie.Subscribe("client1", "test/#", 1)
	trie.Subscribe("client1", "test/specific", 2)

	// When matching, should use highest QoS
	matches := trie.Match("test/specific")
	if matches["client1"] != 2 {
		t.Errorf("expected QoS2 for test/specific, got %d", matches["client1"])
	}
}

func TestValidateTopicName(t *testing.T) {
	valid := []string{
		"sensors/temp",
		"alarm/critical",
		"a/b/c/d/e",
		"single",
	}
	for _, topic := range valid {
		if !broker.ValidateTopicName(topic) {
			t.Errorf("expected valid topic: %q", topic)
		}
	}

	invalid := []string{
		"",
		"sensors/+",
		"alarm/#",
		"test/+/end",
	}
	for _, topic := range invalid {
		if broker.ValidateTopicName(topic) {
			t.Errorf("expected invalid topic: %q", topic)
		}
	}
}

func TestValidateTopicFilter(t *testing.T) {
	valid := []string{
		"sensors/+/temp",
		"alarm/#",
		"#",
		"+",
		"a/b/c",
		"sensors/+",
		"a/+/b/+/c",
	}
	for _, f := range valid {
		if !broker.ValidateTopicFilter(f) {
			t.Errorf("expected valid filter: %q", f)
		}
	}

	invalid := []string{
		"",
		"a/#/b",       // # must be last
		"a/#+",        // # mixed with other chars
		"a/+extra/b",  // + must be whole segment
	}
	for _, f := range invalid {
		if broker.ValidateTopicFilter(f) {
			t.Errorf("expected invalid filter: %q", f)
		}
	}
}
