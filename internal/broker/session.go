// Package broker implements MQTT session management.
package broker

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/sandoe/mqtt-quic-broker/internal/mqtt"
)

// sessionState represents the state of a session.
type sessionState int

const (
	sessionConnected    sessionState = iota
	sessionDisconnected
)

// inflightMessage tracks an in-flight QoS 1/2 message.
type inflightMessage struct {
	msg       *mqtt.Message
	packetID  uint16
	state     int // 0=published, 1=pubrec received (for QoS2)
	timestamp time.Time
}

// Session represents an MQTT client session.
type Session struct {
	mu sync.Mutex

	clientID      string
	cleanStart    bool
	state         sessionState
	expirySeconds uint32

	subscriptions map[string]mqtt.Subscription
	inflight      map[uint16]*inflightMessage
	pendingQoS0   []*mqtt.Message
	pendingQoS1   []*mqtt.Message
	pendingQoS2   []*mqtt.Message

	nextPacketID atomic.Uint32

	lastActivity time.Time
	createdAt    time.Time
	connectedAt  time.Time

	willMessage *mqtt.WillMessage
}

// newSession creates a new Session.
func newSession(clientID string, cleanStart bool, expirySeconds uint32) *Session {
	s := &Session{
		clientID:      clientID,
		cleanStart:    cleanStart,
		expirySeconds: expirySeconds,
		subscriptions: make(map[string]mqtt.Subscription),
		inflight:      make(map[uint16]*inflightMessage),
		createdAt:     time.Now(),
	}
	s.nextPacketID.Store(1)
	return s
}

// ClientID returns the session's client ID.
func (s *Session) ClientID() string { return s.clientID }

// IsCleanStart returns true if session was created with clean start.
func (s *Session) IsCleanStart() bool { return s.cleanStart }

// AddSubscription adds or updates a subscription.
func (s *Session) AddSubscription(sub mqtt.Subscription) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subscriptions[sub.TopicFilter] = sub
}

// RemoveSubscription removes a subscription.
func (s *Session) RemoveSubscription(filter string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.subscriptions, filter)
}

// Subscriptions returns a copy of all subscriptions.
func (s *Session) Subscriptions() []mqtt.Subscription {
	s.mu.Lock()
	defer s.mu.Unlock()
	subs := make([]mqtt.Subscription, 0, len(s.subscriptions))
	for _, sub := range s.subscriptions {
		subs = append(subs, sub)
	}
	return subs
}

// NextPacketID returns the next available packet identifier.
func (s *Session) NextPacketID() uint16 {
	for {
		id := s.nextPacketID.Add(1)
		if id > mqtt.MaxPacketID {
			s.nextPacketID.Store(1)
			id = 1
		}
		s.mu.Lock()
		_, inUse := s.inflight[uint16(id)]
		s.mu.Unlock()
		if !inUse {
			return uint16(id)
		}
	}
}

// AddInflight adds a message to the inflight map.
func (s *Session) AddInflight(packetID uint16, msg *mqtt.Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inflight[packetID] = &inflightMessage{
		msg:       msg,
		packetID:  packetID,
		timestamp: time.Now(),
	}
}

// AckInflight acknowledges and removes an inflight message (QoS 1).
// Returns the message if found.
func (s *Session) AckInflight(packetID uint16) *inflightMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg, ok := s.inflight[packetID]
	if ok {
		delete(s.inflight, packetID)
	}
	return msg
}

// AdvanceInflight marks a QoS 2 message as PUBREC received.
func (s *Session) AdvanceInflight(packetID uint16) *inflightMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg, ok := s.inflight[packetID]
	if ok {
		msg.state = 1
	}
	return msg
}

// CompleteInflight removes a QoS 2 message after PUBCOMP (state=1).
func (s *Session) CompleteInflight(packetID uint16) *inflightMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg, ok := s.inflight[packetID]
	if ok && msg.state == 1 {
		delete(s.inflight, packetID)
		return msg
	}
	return nil
}

// InflightCount returns the number of in-flight messages.
func (s *Session) InflightCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.inflight)
}

// SetWill sets the will message for this session.
func (s *Session) SetWill(will *mqtt.WillMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.willMessage = will
}

// Will returns the will message (if any).
func (s *Session) Will() *mqtt.WillMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.willMessage
}

// MarkActive updates the last activity timestamp.
func (s *Session) MarkActive() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastActivity = time.Now()
}

// IsExpired returns true if the session has expired.
func (s *Session) IsExpired() bool {
	if s.expirySeconds == 0 {
		return true
	}
	if s.expirySeconds == 0xFFFFFFFF {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == sessionConnected {
		return false
	}
	expiry := s.lastActivity.Add(time.Duration(s.expirySeconds) * time.Second)
	return time.Now().After(expiry)
}

// SessionManager manages all sessions.
type SessionManager struct {
	mu       sync.RWMutex
	sessions map[string]*Session
}

// NewSessionManager creates a new SessionManager.
func NewSessionManager() *SessionManager {
	return &SessionManager{
		sessions: make(map[string]*Session),
	}
}

// GetOrCreate returns an existing session or creates a new one.
// Returns (session, sessionPresent).
func (m *SessionManager) GetOrCreate(clientID string, cleanStart bool, expirySeconds uint32) (*Session, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if existing, ok := m.sessions[clientID]; ok && !cleanStart {
		existing.mu.Lock()
		existing.state = sessionConnected
		existing.connectedAt = time.Now()
		existing.mu.Unlock()
		return existing, true
	}

	s := newSession(clientID, cleanStart, expirySeconds)
	s.state = sessionConnected
	s.connectedAt = time.Now()
	m.sessions[clientID] = s
	return s, false
}

// Get returns an existing session or nil.
func (m *SessionManager) Get(clientID string) *Session {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sessions[clientID]
}

// Disconnect marks a session as disconnected.
func (m *SessionManager) Disconnect(clientID string) {
	m.mu.RLock()
	s := m.sessions[clientID]
	m.mu.RUnlock()

	if s == nil {
		return
	}

	s.mu.Lock()
	s.state = sessionDisconnected
	s.lastActivity = time.Now()
	s.mu.Unlock()
}

// Remove removes a session (used for clean start or expired sessions).
func (m *SessionManager) Remove(clientID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, clientID)
}

// CleanExpired removes all expired sessions.
func (m *SessionManager) CleanExpired() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	removed := 0
	for id, s := range m.sessions {
		if s.IsExpired() {
			delete(m.sessions, id)
			removed++
		}
	}
	return removed
}

// Count returns the total number of sessions.
func (m *SessionManager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.sessions)
}
