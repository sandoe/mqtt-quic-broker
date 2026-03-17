package broker_test

import (
	"testing"

	"github.com/sandoe/mqtt-quic-broker/internal/broker"
)

func BenchmarkTopicMatch(b *testing.B) {
	trie := broker.NewTopicTrie()
	// Set up 1000 subscriptions
	for i := 0; i < 1000; i++ {
		trie.Subscribe("client1", "sensors/#", 0)
		trie.Subscribe("client2", "sensors/+/temp", 1)
		trie.Subscribe("client3", "alarm/#", 2)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		trie.Match("sensors/room1/temp")
	}
}

func BenchmarkTopicMatchExact(b *testing.B) {
	trie := broker.NewTopicTrie()
	for i := 0; i < 100; i++ {
		trie.Subscribe("client1", "sensors/temp", 0)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		trie.Match("sensors/temp")
	}
}

func BenchmarkTopicSubscribe(b *testing.B) {
	trie := broker.NewTopicTrie()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		trie.Subscribe("client1", "sensors/#", 0)
	}
}
