package mqtt_test

import (
	"bytes"
	"testing"

	"github.com/sandoe/mqtt-quic-broker/internal/mqtt"
)

func BenchmarkPublishEncode(b *testing.B) {
	pkt := &mqtt.PublishPacket{
		TopicName: "sensors/temperature/device-001",
		Payload:   []byte(`{"ts":1700000000000,"value":22.5,"unit":"C"}`),
		QoS:       mqtt.QoS0,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		buf.Grow(128)
		pkt.Encode(&buf)
	}
}

func BenchmarkPublishDecode(b *testing.B) {
	pkt := &mqtt.PublishPacket{
		TopicName: "sensors/temperature/device-001",
		Payload:   []byte(`{"ts":1700000000000,"value":22.5,"unit":"C"}`),
		QoS:       mqtt.QoS1,
		PacketID:  1,
	}

	var buf bytes.Buffer
	pkt.Encode(&buf)
	encoded := buf.Bytes()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := bytes.NewReader(encoded)
		dec := mqtt.NewDecoder(r)
		dec.Decode()
	}
}

func BenchmarkPublishRoundTrip(b *testing.B) {
	pkt := &mqtt.PublishPacket{
		TopicName: "alarm/critical/zone-1",
		Payload:   []byte(`{"type":"fire","zone":1,"priority":"high"}`),
		QoS:       mqtt.QoS2,
		PacketID:  42,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		pkt.Encode(&buf)
		dec := mqtt.NewDecoder(&buf)
		dec.Decode()
	}
}

func BenchmarkConnectEncode(b *testing.B) {
	exp := uint32(3600)
	pkt := &mqtt.ConnectPacket{
		ProtocolName:  mqtt.ProtocolName,
		ProtocolLevel: mqtt.ProtocolVersion,
		ConnectFlags: mqtt.ConnectFlags{
			CleanStart:   true,
			UsernameFlag: true,
		},
		KeepAlive: 60,
		ClientID:  "bench-client-001",
		Username:  "user",
		Properties: &mqtt.Properties{
			SessionExpiryInterval: &exp,
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		pkt.Encode(&buf)
	}
}
