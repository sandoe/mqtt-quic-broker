// Package broker implements the core MQTT v5.0 broker.
package broker

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/sandoe/mqtt-quic-broker/internal/config"
	"github.com/sandoe/mqtt-quic-broker/internal/metrics"
	"github.com/sandoe/mqtt-quic-broker/internal/mqtt"
)

// clientConn wraps an MQTT connection with session and keepalive state.
type clientConn struct {
	conn      mqtt.Connection
	session   *Session
	keepAlive time.Duration
	cancel    context.CancelFunc
}

// Broker is the core MQTT v5.0 broker.
type Broker struct {
	cfg     *config.Config
	logger  *slog.Logger
	metrics *metrics.Metrics

	sessions *SessionManager
	topics   *TopicTrie

	mu      sync.RWMutex
	clients map[string]*clientConn // clientID -> conn

	priorityPrefixes []string
}

// New creates and returns a new Broker.
func New(cfg *config.Config, m *metrics.Metrics, logger *slog.Logger) *Broker {
	if logger == nil {
		logger = slog.Default()
	}
	b := &Broker{
		cfg:              cfg,
		logger:           logger,
		metrics:          m,
		sessions:         NewSessionManager(),
		topics:           NewTopicTrie(),
		clients:          make(map[string]*clientConn),
		priorityPrefixes: cfg.Broker.PriorityTopics,
	}
	return b
}

// Start begins background goroutines (session cleanup, etc.).
func (b *Broker) Start(ctx context.Context) {
	go b.sessionCleanup(ctx)
}

func (b *Broker) sessionCleanup(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n := b.sessions.CleanExpired()
			if n > 0 {
				b.logger.Info("cleaned expired sessions", "count", n)
			}
		}
	}
}

// HandleConnect processes a CONNECT packet.
func (b *Broker) HandleConnect(conn mqtt.Connection, pkt *mqtt.ConnectPacket) error {
	start := time.Now()
	defer func() {
		if b.metrics != nil {
			b.metrics.ObserveConnectLatency(time.Since(start).Seconds())
		}
	}()

	b.logger.Info("client connecting",
		"client_id", pkt.ClientID,
		"remote", conn.RemoteAddr(),
		"clean_start", pkt.ConnectFlags.CleanStart,
		"protocol_level", pkt.ProtocolLevel,
	)

	// Validate client ID
	if pkt.ClientID == "" && !pkt.ConnectFlags.CleanStart {
		return b.sendConnack(conn, mqtt.ReasonClientIdentifierNotValid, false, nil)
	}

	var expirySeconds uint32
	if pkt.Properties != nil && pkt.Properties.SessionExpiryInterval != nil {
		expirySeconds = *pkt.Properties.SessionExpiryInterval
	}

	// Get or create session
	session, sessionPresent := b.sessions.GetOrCreate(
		pkt.ClientID,
		pkt.ConnectFlags.CleanStart,
		expirySeconds,
	)

	// Set will message
	if pkt.ConnectFlags.WillFlag && pkt.WillMessage != nil {
		session.SetWill(pkt.WillMessage)
	}

	// Register client connection
	keepAlive := time.Duration(pkt.KeepAlive) * time.Second
	if keepAlive == 0 {
		keepAlive = mqtt.DefaultKeepAlive
	}

	ctx, cancel := context.WithCancel(context.Background())
	cc := &clientConn{
		conn:      conn,
		session:   session,
		keepAlive: keepAlive,
		cancel:    cancel,
	}

	// Replace any existing connection for this client
	b.mu.Lock()
	if existing, ok := b.clients[pkt.ClientID]; ok {
		existing.cancel()
		existing.conn.Close()
		b.logger.Info("taking over existing connection", "client_id", pkt.ClientID)
	}
	b.clients[pkt.ClientID] = cc
	b.mu.Unlock()

	// Start keepalive monitor
	go b.keepAliveMonitor(ctx, pkt.ClientID, keepAlive)

	if b.metrics != nil {
		b.metrics.ClientConnected()
	}

	// Build CONNACK properties
	props := &mqtt.Properties{}
	tam := uint16(b.cfg.Broker.TopicAliasMaximum)
	props.TopicAliasMaximum = &tam
	rm := uint16(b.cfg.Broker.ReceiveMaximum)
	props.ReceiveMaximum = &rm

	// Send CONNACK
	if err := b.sendConnack(conn, mqtt.ReasonSuccess, sessionPresent, props); err != nil {
		return err
	}

	// Re-send stored messages for existing session
	if sessionPresent {
		go b.resendInflight(pkt.ClientID)
	}

	// Send retained messages for existing subscriptions
	if sessionPresent {
		for _, sub := range session.Subscriptions() {
			b.sendRetained(conn, sub.TopicFilter, sub.QoS)
		}
	}

	b.logger.Info("client connected",
		"client_id", pkt.ClientID,
		"session_present", sessionPresent,
	)

	return nil
}

func (b *Broker) sendConnack(conn mqtt.Connection, rc mqtt.ReasonCode, sessionPresent bool, props *mqtt.Properties) error {
	return conn.SendPacket(&mqtt.ConnackPacket{
		SessionPresent: sessionPresent,
		ReasonCode:     rc,
		Properties:     props,
	})
}

func (b *Broker) keepAliveMonitor(ctx context.Context, clientID string, keepAlive time.Duration) {
	if keepAlive == 0 {
		return
	}
	// Allow 1.5x keepalive as per MQTT spec
	timeout := keepAlive + keepAlive/2
	ticker := time.NewTicker(timeout)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.mu.RLock()
			cc, ok := b.clients[clientID]
			b.mu.RUnlock()
			if !ok {
				return
			}
			if time.Since(cc.session.lastActivity) > timeout {
				b.logger.Warn("keepalive timeout", "client_id", clientID)
				b.Disconnect(clientID, mqtt.ReasonKeepAliveTimeout)
				return
			}
		}
	}
}

// HandlePublish processes a PUBLISH packet.
func (b *Broker) HandlePublish(conn mqtt.Connection, pkt *mqtt.PublishPacket) error {
	// Validate topic
	if !ValidateTopicName(pkt.TopicName) {
		return b.disconnectConn(conn, mqtt.ReasonTopicNameInvalid)
	}

	// Build internal message
	msg := &mqtt.Message{
		Topic:      pkt.TopicName,
		Payload:    pkt.Payload,
		QoS:        pkt.QoS,
		Retain:     pkt.Retain,
		PacketID:   pkt.PacketID,
		Properties: pkt.Properties,
		Priority:   b.messagePriority(pkt.TopicName),
	}

	// Handle retained messages
	if pkt.Retain {
		b.topics.SetRetained(pkt.TopicName, pkt.Payload, byte(pkt.QoS))
	}

	// QoS 0: fire and forget
	if pkt.QoS == mqtt.QoS0 {
		b.route(msg)
		if b.metrics != nil {
			b.metrics.MessagePublished(pkt.TopicName, 0)
		}
		return nil
	}

	// QoS 1: send PUBACK
	if pkt.QoS == mqtt.QoS1 {
		b.route(msg)
		if b.metrics != nil {
			b.metrics.MessagePublished(pkt.TopicName, 1)
		}
		return conn.SendPacket(&mqtt.PubackPacket{
			FixedHeader: mqtt.FixedHeader{PacketType: mqtt.PacketPUBACK},
			PacketID:    pkt.PacketID,
			ReasonCode:  mqtt.ReasonSuccess,
		})
	}

	// QoS 2: send PUBREC, wait for PUBREL
	if pkt.QoS == mqtt.QoS2 {
		// Store in session for QoS 2 flow
		clientID := b.clientIDForConn(conn)
		if clientID != "" {
			if s := b.sessions.Get(clientID); s != nil {
				s.AddInflight(pkt.PacketID, msg)
			}
		}
		if b.metrics != nil {
			b.metrics.MessagePublished(pkt.TopicName, 2)
		}
		return conn.SendPacket(&mqtt.PubackPacket{
			FixedHeader: mqtt.FixedHeader{PacketType: mqtt.PacketPUBREC},
			PacketID:    pkt.PacketID,
			ReasonCode:  mqtt.ReasonSuccess,
		})
	}

	return nil
}

// HandlePuback processes PUBACK/PUBREC/PUBREL/PUBCOMP packets.
func (b *Broker) HandlePuback(conn mqtt.Connection, pkt *mqtt.PubackPacket) error {
	clientID := b.clientIDForConn(conn)
	if clientID == "" {
		return nil
	}
	s := b.sessions.Get(clientID)
	if s == nil {
		return nil
	}

	switch pkt.FixedHeader.PacketType {
	case mqtt.PacketPUBACK:
		s.AckInflight(pkt.PacketID)

	case mqtt.PacketPUBREC:
		// Client received our PUBLISH QoS2, we send PUBREL
		s.AdvanceInflight(pkt.PacketID)
		return conn.SendPacket(&mqtt.PubackPacket{
			FixedHeader: mqtt.FixedHeader{PacketType: mqtt.PacketPUBREL},
			PacketID:    pkt.PacketID,
			ReasonCode:  mqtt.ReasonSuccess,
		})

	case mqtt.PacketPUBREL:
		// Broker received PUBREL for an incoming QoS2 PUBLISH
		// Now route the stored message and send PUBCOMP
		if im := s.CompleteInflight(pkt.PacketID); im != nil {
			b.route(im.msg)
		}
		return conn.SendPacket(&mqtt.PubackPacket{
			FixedHeader: mqtt.FixedHeader{PacketType: mqtt.PacketPUBCOMP},
			PacketID:    pkt.PacketID,
			ReasonCode:  mqtt.ReasonSuccess,
		})

	case mqtt.PacketPUBCOMP:
		s.AckInflight(pkt.PacketID)
	}

	return nil
}

// HandleSubscribe processes a SUBSCRIBE packet.
func (b *Broker) HandleSubscribe(conn mqtt.Connection, pkt *mqtt.SubscribePacket) error {
	clientID := b.clientIDForConn(conn)
	if clientID == "" {
		return fmt.Errorf("subscribe from unknown connection")
	}

	session := b.sessions.Get(clientID)
	if session == nil {
		return fmt.Errorf("no session for client %s", clientID)
	}

	reasonCodes := make([]mqtt.ReasonCode, len(pkt.Subscriptions))

	for i, sub := range pkt.Subscriptions {
		if !ValidateTopicFilter(sub.TopicFilter) {
			reasonCodes[i] = mqtt.ReasonTopicFilterInvalid
			continue
		}

		if sub.QoS > mqtt.QoS2 {
			reasonCodes[i] = mqtt.ReasonQoSNotSupported
			continue
		}

		// Add to topic trie
		b.topics.Subscribe(clientID, sub.TopicFilter, byte(sub.QoS))

		// Store in session
		session.AddSubscription(sub)

		// Grant requested QoS
		switch sub.QoS {
		case mqtt.QoS0:
			reasonCodes[i] = mqtt.ReasonGrantedQoS0
		case mqtt.QoS1:
			reasonCodes[i] = mqtt.ReasonGrantedQoS1
		case mqtt.QoS2:
			reasonCodes[i] = mqtt.ReasonGrantedQoS2
		}

		b.logger.Debug("client subscribed",
			"client_id", clientID,
			"filter", sub.TopicFilter,
			"qos", sub.QoS,
		)

		// Send matching retained messages
		if sub.RetainHandling != 2 {
			b.sendRetained(conn, sub.TopicFilter, sub.QoS)
		}
	}

	return conn.SendPacket(&mqtt.SubackPacket{
		PacketID:    pkt.PacketID,
		ReasonCodes: reasonCodes,
	})
}

// HandleUnsubscribe processes an UNSUBSCRIBE packet.
func (b *Broker) HandleUnsubscribe(conn mqtt.Connection, pkt *mqtt.UnsubscribePacket) error {
	clientID := b.clientIDForConn(conn)
	if clientID == "" {
		return nil
	}

	session := b.sessions.Get(clientID)
	reasonCodes := make([]mqtt.ReasonCode, len(pkt.TopicFilters))

	for i, filter := range pkt.TopicFilters {
		b.topics.Unsubscribe(clientID, filter)
		if session != nil {
			session.RemoveSubscription(filter)
		}
		reasonCodes[i] = mqtt.ReasonSuccess
	}

	return conn.SendPacket(&mqtt.UnsubackPacket{
		PacketID:    pkt.PacketID,
		ReasonCodes: reasonCodes,
	})
}

// HandlePingreq processes a PINGREQ packet.
func (b *Broker) HandlePingreq(conn mqtt.Connection, _ *mqtt.PingreqPacket) error {
	clientID := b.clientIDForConn(conn)
	if s := b.sessions.Get(clientID); s != nil {
		s.MarkActive()
	}
	return conn.SendPacket(&mqtt.PingrespPacket{})
}

// HandleDisconnect processes a DISCONNECT packet.
func (b *Broker) HandleDisconnect(conn mqtt.Connection, pkt *mqtt.DisconnectPacket) error {
	clientID := b.clientIDForConn(conn)
	b.logger.Info("client disconnecting",
		"client_id", clientID,
		"reason_code", pkt.ReasonCode,
	)

	// If reason is normal disconnection, clear the will
	if pkt.ReasonCode == mqtt.ReasonNormalDisconnection {
		if s := b.sessions.Get(clientID); s != nil {
			s.SetWill(nil)
		}
	}

	b.removeClient(clientID)
	return nil
}

// Disconnect forcibly disconnects a client.
func (b *Broker) Disconnect(clientID string, reason mqtt.ReasonCode) {
	b.mu.RLock()
	cc, ok := b.clients[clientID]
	b.mu.RUnlock()

	if ok {
		cc.conn.SendPacket(&mqtt.DisconnectPacket{ReasonCode: reason})
		cc.conn.Close()
	}

	b.removeClient(clientID)

	// Publish will message
	if s := b.sessions.Get(clientID); s != nil {
		if will := s.Will(); will != nil {
			b.publishWill(will)
		}
	}
}

func (b *Broker) removeClient(clientID string) {
	b.mu.Lock()
	cc, ok := b.clients[clientID]
	if ok {
		cc.cancel()
		delete(b.clients, clientID)
	}
	b.mu.Unlock()

	b.sessions.Disconnect(clientID)
	// Remove from topic trie if session was clean
	if s := b.sessions.Get(clientID); s != nil && s.IsCleanStart() {
		b.topics.UnsubscribeAll(clientID)
		b.sessions.Remove(clientID)
	}

	if b.metrics != nil {
		b.metrics.ClientDisconnected()
	}
}

func (b *Broker) publishWill(will *mqtt.WillMessage) {
	msg := &mqtt.Message{
		Topic:      will.Topic,
		Payload:    will.Payload,
		QoS:        will.QoS,
		Retain:     will.Retain,
		Properties: will.Properties,
		Priority:   b.messagePriority(will.Topic),
	}
	if will.Retain {
		b.topics.SetRetained(will.Topic, will.Payload, byte(will.QoS))
	}
	b.route(msg)
}

// route distributes a message to all matching subscribers.
func (b *Broker) route(msg *mqtt.Message) {
	matches := b.topics.Match(msg.Topic)
	if len(matches) == 0 {
		return
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	for clientID, subQoS := range matches {
		cc, ok := b.clients[clientID]
		if !ok {
			continue
		}

		// Effective QoS is min(publish QoS, subscription QoS)
		effectiveQoS := mqtt.QoS(subQoS)
		if msg.QoS < effectiveQoS {
			effectiveQoS = msg.QoS
		}

		pub := &mqtt.PublishPacket{
			TopicName:  msg.Topic,
			Payload:    msg.Payload,
			QoS:        effectiveQoS,
			Retain:     false,
			Properties: msg.Properties,
		}

		if effectiveQoS > mqtt.QoS0 {
			pid := cc.session.NextPacketID()
			pub.PacketID = pid
			cc.session.AddInflight(pid, msg)
		}

		if err := cc.conn.SendPacket(pub); err != nil {
			b.logger.Error("failed to deliver message",
				"client_id", clientID,
				"topic", msg.Topic,
				"error", err,
			)
		}
	}

	if b.metrics != nil {
		b.metrics.MessageDelivered(msg.Topic, len(matches))
	}
}

func (b *Broker) sendRetained(conn mqtt.Connection, filter string, qos mqtt.QoS) {
	retained := b.topics.GetRetained(filter)
	for _, r := range retained {
		effectiveQoS := mqtt.QoS(r.QoS)
		if qos < effectiveQoS {
			effectiveQoS = qos
		}
		pub := &mqtt.PublishPacket{
			TopicName: r.Topic,
			Payload:   r.Payload,
			QoS:       effectiveQoS,
			Retain:    true,
		}
		if err := conn.SendPacket(pub); err != nil {
			b.logger.Error("failed to send retained message",
				"topic", r.Topic,
				"error", err,
			)
		}
	}
}

func (b *Broker) resendInflight(clientID string) {
	s := b.sessions.Get(clientID)
	if s == nil {
		return
	}

	b.mu.RLock()
	cc, ok := b.clients[clientID]
	b.mu.RUnlock()
	if !ok {
		return
	}

	s.mu.Lock()
	msgs := make([]*inflightMessage, 0, len(s.inflight))
	for _, im := range s.inflight {
		msgs = append(msgs, im)
	}
	s.mu.Unlock()

	for _, im := range msgs {
		pub := &mqtt.PublishPacket{
			TopicName:  im.msg.Topic,
			Payload:    im.msg.Payload,
			QoS:        im.msg.QoS,
			PacketID:   im.packetID,
			Dup:        true,
			Properties: im.msg.Properties,
		}
		cc.conn.SendPacket(pub)
	}
}

func (b *Broker) clientIDForConn(conn mqtt.Connection) string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for id, cc := range b.clients {
		if cc.conn == conn {
			return id
		}
	}
	return ""
}

func (b *Broker) messagePriority(topic string) mqtt.Priority {
	for _, prefix := range b.priorityPrefixes {
		if strings.HasPrefix(topic, prefix) {
			return mqtt.PriorityCritical
		}
	}
	return mqtt.PriorityNormal
}

func (b *Broker) disconnectConn(conn mqtt.Connection, rc mqtt.ReasonCode) error {
	conn.SendPacket(&mqtt.DisconnectPacket{ReasonCode: rc})
	return conn.Close()
}

// ClientCount returns the number of connected clients.
func (b *Broker) ClientCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.clients)
}

// SessionCount returns the total number of sessions.
func (b *Broker) SessionCount() int {
	return b.sessions.Count()
}
