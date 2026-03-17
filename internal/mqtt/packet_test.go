package mqtt_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/sandoe/mqtt-quic-broker/internal/mqtt"
)

// roundTrip encodes then decodes a packet, returning the decoded packet.
func roundTrip(t *testing.T, pkt mqtt.Packet) mqtt.Packet {
	t.Helper()
	var buf bytes.Buffer
	if err := pkt.Encode(&buf); err != nil {
		t.Fatalf("encode %T: %v", pkt, err)
	}
	dec := mqtt.NewDecoder(&buf)
	got, err := dec.Decode()
	if err != nil {
		t.Fatalf("decode %T: %v", pkt, err)
	}
	return got
}

func TestConnectRoundTrip(t *testing.T) {
	exp := uint32(300)
	pkt := &mqtt.ConnectPacket{
		ProtocolName:  mqtt.ProtocolName,
		ProtocolLevel: mqtt.ProtocolVersion,
		ConnectFlags: mqtt.ConnectFlags{
			CleanStart:   true,
			UsernameFlag: true,
			PasswordFlag: true,
		},
		KeepAlive: 60,
		ClientID:  "test-client",
		Username:  "user1",
		Password:  []byte("pass1"),
		Properties: &mqtt.Properties{
			SessionExpiryInterval: &exp,
		},
	}

	got := roundTrip(t, pkt).(*mqtt.ConnectPacket)

	if got.ProtocolName != mqtt.ProtocolName {
		t.Errorf("ProtocolName: got %q, want %q", got.ProtocolName, mqtt.ProtocolName)
	}
	if got.ProtocolLevel != mqtt.ProtocolVersion {
		t.Errorf("ProtocolLevel: got %d, want %d", got.ProtocolLevel, mqtt.ProtocolVersion)
	}
	if got.ClientID != pkt.ClientID {
		t.Errorf("ClientID: got %q, want %q", got.ClientID, pkt.ClientID)
	}
	if got.Username != pkt.Username {
		t.Errorf("Username: got %q, want %q", got.Username, pkt.Username)
	}
	if string(got.Password) != string(pkt.Password) {
		t.Errorf("Password: got %q, want %q", got.Password, pkt.Password)
	}
	if got.KeepAlive != pkt.KeepAlive {
		t.Errorf("KeepAlive: got %d, want %d", got.KeepAlive, pkt.KeepAlive)
	}
	if !got.ConnectFlags.CleanStart {
		t.Error("CleanStart should be true")
	}
	if got.Properties == nil || got.Properties.SessionExpiryInterval == nil {
		t.Fatal("missing SessionExpiryInterval property")
	}
	if *got.Properties.SessionExpiryInterval != exp {
		t.Errorf("SessionExpiryInterval: got %d, want %d", *got.Properties.SessionExpiryInterval, exp)
	}
}

func TestConnectWithWill(t *testing.T) {
	pkt := &mqtt.ConnectPacket{
		ProtocolName:  mqtt.ProtocolName,
		ProtocolLevel: mqtt.ProtocolVersion,
		ConnectFlags: mqtt.ConnectFlags{
			CleanStart: true,
			WillFlag:   true,
			WillQoS:    mqtt.QoS1,
			WillRetain: false,
		},
		KeepAlive: 30,
		ClientID:  "will-client",
		WillMessage: &mqtt.WillMessage{
			Topic:   "lwt/will-client",
			Payload: []byte("offline"),
			QoS:     mqtt.QoS1,
		},
	}

	got := roundTrip(t, pkt).(*mqtt.ConnectPacket)

	if !got.ConnectFlags.WillFlag {
		t.Error("WillFlag should be true")
	}
	if got.WillMessage == nil {
		t.Fatal("WillMessage should not be nil")
	}
	if got.WillMessage.Topic != pkt.WillMessage.Topic {
		t.Errorf("Will.Topic: got %q, want %q", got.WillMessage.Topic, pkt.WillMessage.Topic)
	}
	if string(got.WillMessage.Payload) != string(pkt.WillMessage.Payload) {
		t.Errorf("Will.Payload: got %q, want %q", got.WillMessage.Payload, pkt.WillMessage.Payload)
	}
}

func TestConnackRoundTrip(t *testing.T) {
	tam := uint16(100)
	pkt := &mqtt.ConnackPacket{
		SessionPresent: true,
		ReasonCode:     mqtt.ReasonSuccess,
		Properties: &mqtt.Properties{
			TopicAliasMaximum: &tam,
		},
	}

	got := roundTrip(t, pkt).(*mqtt.ConnackPacket)

	if got.SessionPresent != pkt.SessionPresent {
		t.Errorf("SessionPresent: got %v, want %v", got.SessionPresent, pkt.SessionPresent)
	}
	if got.ReasonCode != pkt.ReasonCode {
		t.Errorf("ReasonCode: got %d, want %d", got.ReasonCode, pkt.ReasonCode)
	}
	if got.Properties == nil || got.Properties.TopicAliasMaximum == nil {
		t.Fatal("missing TopicAliasMaximum")
	}
	if *got.Properties.TopicAliasMaximum != tam {
		t.Errorf("TopicAliasMaximum: got %d, want %d", *got.Properties.TopicAliasMaximum, tam)
	}
}

func TestPublishQoS0RoundTrip(t *testing.T) {
	pkt := &mqtt.PublishPacket{
		TopicName: "test/topic",
		Payload:   []byte(`{"temp":22.5}`),
		QoS:       mqtt.QoS0,
		Retain:    false,
	}

	got := roundTrip(t, pkt).(*mqtt.PublishPacket)

	if got.TopicName != pkt.TopicName {
		t.Errorf("TopicName: got %q, want %q", got.TopicName, pkt.TopicName)
	}
	if string(got.Payload) != string(pkt.Payload) {
		t.Errorf("Payload: got %q, want %q", got.Payload, pkt.Payload)
	}
	if got.QoS != pkt.QoS {
		t.Errorf("QoS: got %d, want %d", got.QoS, pkt.QoS)
	}
}

func TestPublishQoS1RoundTrip(t *testing.T) {
	pkt := &mqtt.PublishPacket{
		TopicName: "sensors/pressure",
		Payload:   []byte("1013.25 hPa"),
		QoS:       mqtt.QoS1,
		PacketID:  42,
		Retain:    true,
		Dup:       false,
	}

	got := roundTrip(t, pkt).(*mqtt.PublishPacket)

	if got.PacketID != pkt.PacketID {
		t.Errorf("PacketID: got %d, want %d", got.PacketID, pkt.PacketID)
	}
	if !got.Retain {
		t.Error("Retain should be true")
	}
	if got.QoS != mqtt.QoS1 {
		t.Errorf("QoS: got %d, want %d", got.QoS, mqtt.QoS1)
	}
}

func TestPublishQoS2RoundTrip(t *testing.T) {
	pkt := &mqtt.PublishPacket{
		TopicName: "alarm/critical/zone-1",
		Payload:   []byte(`{"type":"fire","zone":1}`),
		QoS:       mqtt.QoS2,
		PacketID:  999,
	}

	got := roundTrip(t, pkt).(*mqtt.PublishPacket)

	if got.QoS != mqtt.QoS2 {
		t.Errorf("QoS: got %d, want %d", got.QoS, mqtt.QoS2)
	}
	if got.PacketID != pkt.PacketID {
		t.Errorf("PacketID: got %d, want %d", got.PacketID, pkt.PacketID)
	}
}

func TestPubackRoundTrip(t *testing.T) {
	types := []mqtt.PacketType{
		mqtt.PacketPUBACK,
		mqtt.PacketPUBREC,
		mqtt.PacketPUBREL,
		mqtt.PacketPUBCOMP,
	}

	for _, pt := range types {
		pkt := &mqtt.PubackPacket{
			FixedHeader: mqtt.FixedHeader{PacketType: pt},
			PacketID:    12345,
			ReasonCode:  mqtt.ReasonSuccess,
		}

		got := roundTrip(t, pkt).(*mqtt.PubackPacket)
		if got.PacketID != pkt.PacketID {
			t.Errorf("%v PacketID: got %d, want %d", pt, got.PacketID, pkt.PacketID)
		}
	}
}

func TestSubscribeRoundTrip(t *testing.T) {
	pkt := &mqtt.SubscribePacket{
		PacketID: 7,
		Subscriptions: []mqtt.Subscription{
			{TopicFilter: "sensors/#", QoS: mqtt.QoS1},
			{TopicFilter: "alarm/+/critical", QoS: mqtt.QoS2, NoLocal: true},
		},
	}

	got := roundTrip(t, pkt).(*mqtt.SubscribePacket)

	if got.PacketID != pkt.PacketID {
		t.Errorf("PacketID: got %d, want %d", got.PacketID, pkt.PacketID)
	}
	if len(got.Subscriptions) != len(pkt.Subscriptions) {
		t.Fatalf("Subscriptions: got %d, want %d", len(got.Subscriptions), len(pkt.Subscriptions))
	}
	if got.Subscriptions[0].TopicFilter != "sensors/#" {
		t.Errorf("Subscriptions[0].Filter: got %q", got.Subscriptions[0].TopicFilter)
	}
	if got.Subscriptions[1].NoLocal != true {
		t.Error("Subscriptions[1].NoLocal should be true")
	}
}

func TestSubackRoundTrip(t *testing.T) {
	pkt := &mqtt.SubackPacket{
		PacketID:    7,
		ReasonCodes: []mqtt.ReasonCode{mqtt.ReasonGrantedQoS1, mqtt.ReasonGrantedQoS2},
	}

	got := roundTrip(t, pkt).(*mqtt.SubackPacket)

	if len(got.ReasonCodes) != len(pkt.ReasonCodes) {
		t.Fatalf("ReasonCodes length: got %d, want %d", len(got.ReasonCodes), len(pkt.ReasonCodes))
	}
	for i, rc := range pkt.ReasonCodes {
		if got.ReasonCodes[i] != rc {
			t.Errorf("ReasonCodes[%d]: got %d, want %d", i, got.ReasonCodes[i], rc)
		}
	}
}

func TestUnsubscribeRoundTrip(t *testing.T) {
	pkt := &mqtt.UnsubscribePacket{
		PacketID:     8,
		TopicFilters: []string{"sensors/#", "alarm/+"},
	}

	got := roundTrip(t, pkt).(*mqtt.UnsubscribePacket)

	if len(got.TopicFilters) != len(pkt.TopicFilters) {
		t.Fatalf("TopicFilters length: got %d, want %d", len(got.TopicFilters), len(pkt.TopicFilters))
	}
	for i, f := range pkt.TopicFilters {
		if got.TopicFilters[i] != f {
			t.Errorf("TopicFilters[%d]: got %q, want %q", i, got.TopicFilters[i], f)
		}
	}
}

func TestPingreqPingrespRoundTrip(t *testing.T) {
	req := &mqtt.PingreqPacket{}
	gotReq := roundTrip(t, req)
	if _, ok := gotReq.(*mqtt.PingreqPacket); !ok {
		t.Errorf("expected *PingreqPacket, got %T", gotReq)
	}

	resp := &mqtt.PingrespPacket{}
	gotResp := roundTrip(t, resp)
	if _, ok := gotResp.(*mqtt.PingrespPacket); !ok {
		t.Errorf("expected *PingrespPacket, got %T", gotResp)
	}
}

func TestDisconnectRoundTrip(t *testing.T) {
	pkt := &mqtt.DisconnectPacket{
		ReasonCode: mqtt.ReasonNormalDisconnection,
	}

	var buf bytes.Buffer
	if err := pkt.Encode(&buf); err != nil {
		t.Fatalf("encode: %v", err)
	}

	dec := mqtt.NewDecoder(&buf)
	got, err := dec.Decode()
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	d, ok := got.(*mqtt.DisconnectPacket)
	if !ok {
		t.Fatalf("expected *DisconnectPacket, got %T", got)
	}
	if d.ReasonCode != pkt.ReasonCode {
		t.Errorf("ReasonCode: got %d, want %d", d.ReasonCode, pkt.ReasonCode)
	}
}

func TestDecodeUnknownProtocol(t *testing.T) {
	pkt := &mqtt.ConnectPacket{
		ProtocolName:  "FAKE",
		ProtocolLevel: 4,
		KeepAlive:     60,
		ClientID:      "test",
	}
	var buf bytes.Buffer
	pkt.Encode(&buf)

	dec := mqtt.NewDecoder(&buf)
	_, err := dec.Decode()
	if err == nil {
		t.Fatal("expected error for unsupported protocol")
	}
}

func TestDecodeEOF(t *testing.T) {
	dec := mqtt.NewDecoder(bytes.NewReader(nil))
	_, err := dec.Decode()
	if err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}
}

func TestPublishUserProperties(t *testing.T) {
	pkt := &mqtt.PublishPacket{
		TopicName: "test/props",
		Payload:   []byte("hello"),
		QoS:       mqtt.QoS0,
		Properties: &mqtt.Properties{
			UserProperties: []mqtt.UserProperty{
				{Key: "device", Value: "sensor-001"},
				{Key: "site", Value: "plant-A"},
			},
			ContentType: "application/json",
		},
	}

	got := roundTrip(t, pkt).(*mqtt.PublishPacket)

	if got.Properties == nil {
		t.Fatal("Properties should not be nil")
	}
	if len(got.Properties.UserProperties) != 2 {
		t.Fatalf("UserProperties: got %d, want 2", len(got.Properties.UserProperties))
	}
	if got.Properties.UserProperties[0].Key != "device" {
		t.Errorf("UserProperties[0].Key: got %q, want %q", got.Properties.UserProperties[0].Key, "device")
	}
	if got.Properties.ContentType != "application/json" {
		t.Errorf("ContentType: got %q", got.Properties.ContentType)
	}
}
