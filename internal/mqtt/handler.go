// Package mqtt provides MQTT v5.0 packet dispatch and handling logic.
package mqtt

import (
	"fmt"
	"io"
	"log/slog"
)

// Handler is implemented by types that process MQTT packets.
type Handler interface {
	HandleConnect(conn Connection, pkt *ConnectPacket) error
	HandlePublish(conn Connection, pkt *PublishPacket) error
	HandleSubscribe(conn Connection, pkt *SubscribePacket) error
	HandleUnsubscribe(conn Connection, pkt *UnsubscribePacket) error
	HandlePingreq(conn Connection, pkt *PingreqPacket) error
	HandleDisconnect(conn Connection, pkt *DisconnectPacket) error
	HandlePuback(conn Connection, pkt *PubackPacket) error
}

// Connection is the interface implemented by transport connections.
type Connection interface {
	// SendPacket encodes and sends a packet to the remote peer.
	SendPacket(pkt Packet) error
	// RemoteAddr returns a human-readable remote address string.
	RemoteAddr() string
	// Close closes the connection.
	Close() error
}

// Dispatcher reads packets from a connection and dispatches to a Handler.
type Dispatcher struct {
	logger  *slog.Logger
	handler Handler
}

// NewDispatcher creates a new Dispatcher.
func NewDispatcher(handler Handler, logger *slog.Logger) *Dispatcher {
	if logger == nil {
		logger = slog.Default()
	}
	return &Dispatcher{handler: handler, logger: logger}
}

// Serve reads packets from the reader and dispatches them until EOF or error.
func (d *Dispatcher) Serve(conn Connection, r io.Reader) error {
	dec := NewDecoder(r)
	for {
		pkt, err := dec.Decode()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("decode: %w", err)
		}

		if err := d.dispatch(conn, pkt); err != nil {
			return fmt.Errorf("dispatch %T: %w", pkt, err)
		}
	}
}

func (d *Dispatcher) dispatch(conn Connection, pkt Packet) error {
	d.logger.Debug("dispatching packet",
		"type", pkt.Type(),
		"remote", conn.RemoteAddr(),
	)

	switch p := pkt.(type) {
	case *ConnectPacket:
		return d.handler.HandleConnect(conn, p)
	case *PublishPacket:
		return d.handler.HandlePublish(conn, p)
	case *SubscribePacket:
		return d.handler.HandleSubscribe(conn, p)
	case *UnsubscribePacket:
		return d.handler.HandleUnsubscribe(conn, p)
	case *PingreqPacket:
		return d.handler.HandlePingreq(conn, p)
	case *DisconnectPacket:
		return d.handler.HandleDisconnect(conn, p)
	case *PubackPacket:
		return d.handler.HandlePuback(conn, p)
	default:
		d.logger.Warn("unhandled packet type", "type", pkt.Type())
		return nil
	}
}
