// Package client provides a simple QUIC MQTT v5.0 client library.
package client

import (
"context"
"crypto/tls"
"fmt"
"io"
"net"
"sync"
"sync/atomic"
"time"

"github.com/quic-go/quic-go"
"github.com/sandoe/mqtt-quic-broker/internal/mqtt"
)

// Options contains client connection options.
type Options struct {
// BrokerAddr is the broker address (e.g. "localhost:14567").
BrokerAddr string
// ClientID is the MQTT client ID.
ClientID string
// Username and Password for authentication.
Username string
Password string
// TLSConfig is the TLS configuration. Required for QUIC.
TLSConfig *tls.Config
// KeepAlive is the MQTT keepalive interval (default: 60s).
KeepAlive time.Duration
// CleanStart resets the session on connect.
CleanStart bool
// SessionExpiry is the session expiry interval in seconds.
SessionExpiry uint32
// ConnectTimeout is the timeout for the initial CONNECT/CONNACK exchange.
ConnectTimeout time.Duration
// OnMessage is called for each received PUBLISH message.
OnMessage func(topic string, payload []byte, qos mqtt.QoS)
}

// Client is an MQTT v5.0 client over QUIC.
type Client struct {
opts      Options
conn      *quic.Conn
stream    *quic.Stream
dec       *mqtt.Decoder
mu        sync.Mutex
nextPktID atomic.Uint32
connected bool
done      chan struct{}
}

// Connect establishes a QUIC connection and sends an MQTT CONNECT packet.
func Connect(ctx context.Context, opts Options) (*Client, error) {
if opts.KeepAlive == 0 {
opts.KeepAlive = 60 * time.Second
}
if opts.ConnectTimeout == 0 {
opts.ConnectTimeout = 10 * time.Second
}
if opts.TLSConfig == nil {
opts.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS13}
}

tlsCfg := opts.TLSConfig.Clone()
if len(tlsCfg.NextProtos) == 0 {
tlsCfg.NextProtos = []string{"mqtt"}
}

quicCfg := &quic.Config{
MaxIdleTimeout:  90 * time.Second,
KeepAlivePeriod: 30 * time.Second,
EnableDatagrams: true,
}

conn, err := quic.DialAddr(ctx, opts.BrokerAddr, tlsCfg, quicCfg)
if err != nil {
return nil, fmt.Errorf("quic dial: %w", err)
}

stream, err := conn.OpenStreamSync(ctx)
if err != nil {
conn.CloseWithError(0, "stream failed")
return nil, fmt.Errorf("open stream: %w", err)
}

c := &Client{
opts:   opts,
conn:   conn,
stream: stream,
dec:    mqtt.NewDecoder(stream),
done:   make(chan struct{}),
}
c.nextPktID.Store(1)

// Send CONNECT
connectPkt := buildConnect(opts)
if err := connectPkt.Encode(stream); err != nil {
conn.CloseWithError(0, "connect failed")
return nil, fmt.Errorf("send CONNECT: %w", err)
}

// Wait for CONNACK with timeout
connectCtx, cancel := context.WithTimeout(ctx, opts.ConnectTimeout)
defer cancel()

connackCh := make(chan error, 1)
go func() {
pkt, err := c.dec.Decode()
if err != nil {
connackCh <- err
return
}
ca, ok := pkt.(*mqtt.ConnackPacket)
if !ok {
connackCh <- fmt.Errorf("expected CONNACK, got %T", pkt)
return
}
if ca.ReasonCode != mqtt.ReasonSuccess {
connackCh <- fmt.Errorf("connection refused: reason code %d", ca.ReasonCode)
return
}
connackCh <- nil
}()

select {
case <-connectCtx.Done():
conn.CloseWithError(0, "connect timeout")
return nil, fmt.Errorf("connect timeout")
case err := <-connackCh:
if err != nil {
conn.CloseWithError(0, "connack error")
return nil, err
}
}

c.connected = true
go c.readLoop()

return c, nil
}

func buildConnect(opts Options) *mqtt.ConnectPacket {
pkt := &mqtt.ConnectPacket{
ProtocolName:  mqtt.ProtocolName,
ProtocolLevel: mqtt.ProtocolVersion,
ConnectFlags: mqtt.ConnectFlags{
CleanStart:   opts.CleanStart,
UsernameFlag: opts.Username != "",
PasswordFlag: opts.Password != "",
},
KeepAlive: uint16(opts.KeepAlive / time.Second),
ClientID:  opts.ClientID,
Username:  opts.Username,
Password:  []byte(opts.Password),
}
if opts.SessionExpiry > 0 {
exp := opts.SessionExpiry
pkt.Properties = &mqtt.Properties{
SessionExpiryInterval: &exp,
}
}
return pkt
}

// Publish publishes a message to the given topic.
func (c *Client) Publish(ctx context.Context, topic string, payload []byte, qos mqtt.QoS) error {
if !c.connected {
return fmt.Errorf("not connected")
}

pkt := &mqtt.PublishPacket{
TopicName: topic,
Payload:   payload,
QoS:       qos,
}

if qos > mqtt.QoS0 {
pkt.PacketID = c.nextPacketID()
}

c.mu.Lock()
defer c.mu.Unlock()
return pkt.Encode(c.stream)
}

// Subscribe subscribes to the given topic filter.
func (c *Client) Subscribe(ctx context.Context, filter string, qos mqtt.QoS) error {
if !c.connected {
return fmt.Errorf("not connected")
}

pkt := &mqtt.SubscribePacket{
PacketID: c.nextPacketID(),
Subscriptions: []mqtt.Subscription{
{TopicFilter: filter, QoS: qos},
},
}

c.mu.Lock()
defer c.mu.Unlock()
return pkt.Encode(c.stream)
}

// Disconnect sends a DISCONNECT packet and closes the connection.
func (c *Client) Disconnect() error {
if !c.connected {
return nil
}
c.connected = false

pkt := &mqtt.DisconnectPacket{ReasonCode: mqtt.ReasonNormalDisconnection}
c.mu.Lock()
pkt.Encode(c.stream) //nolint:errcheck
c.mu.Unlock()

close(c.done)
return c.conn.CloseWithError(0, "normal disconnect")
}

func (c *Client) readLoop() {
defer func() {
c.connected = false
}()

for {
select {
case <-c.done:
return
default:
}

pkt, err := c.dec.Decode()
if err != nil {
if err == io.EOF {
return
}
return
}

switch p := pkt.(type) {
case *mqtt.PublishPacket:
c.handlePublish(p)
case *mqtt.PubackPacket:
c.handlePuback(p)
case *mqtt.PingrespPacket:
// keepalive response
case *mqtt.DisconnectPacket:
return
}
}
}

func (c *Client) handlePublish(p *mqtt.PublishPacket) {
if c.opts.OnMessage != nil {
c.opts.OnMessage(p.TopicName, p.Payload, p.QoS)
}

switch p.QoS {
case mqtt.QoS1:
ack := &mqtt.PubackPacket{
FixedHeader: mqtt.FixedHeader{PacketType: mqtt.PacketPUBACK},
PacketID:    p.PacketID,
}
c.mu.Lock()
ack.Encode(c.stream) //nolint:errcheck
c.mu.Unlock()

case mqtt.QoS2:
rec := &mqtt.PubackPacket{
FixedHeader: mqtt.FixedHeader{PacketType: mqtt.PacketPUBREC},
PacketID:    p.PacketID,
}
c.mu.Lock()
rec.Encode(c.stream) //nolint:errcheck
c.mu.Unlock()
}
}

func (c *Client) handlePuback(p *mqtt.PubackPacket) {
switch p.FixedHeader.PacketType {
case mqtt.PacketPUBREL:
comp := &mqtt.PubackPacket{
FixedHeader: mqtt.FixedHeader{PacketType: mqtt.PacketPUBCOMP},
PacketID:    p.PacketID,
}
c.mu.Lock()
comp.Encode(c.stream) //nolint:errcheck
c.mu.Unlock()
}
}

func (c *Client) nextPacketID() uint16 {
id := c.nextPktID.Add(1)
if id > mqtt.MaxPacketID {
c.nextPktID.CompareAndSwap(id, 1)
return 1
}
return uint16(id)
}

// Addr returns the remote broker address.
func (c *Client) Addr() net.Addr {
return c.conn.RemoteAddr()
}
