// Package transport provides transport-agnostic connection interfaces.
package transport

import (
	"context"
	"net"
)

// Listener is the interface implemented by all transport listeners.
type Listener interface {
	// Accept waits for and returns the next connection.
	Accept(ctx context.Context) (Conn, error)
	// Addr returns the listener's network address.
	Addr() net.Addr
	// Close stops the listener.
	Close() error
}

// Conn is the interface implemented by transport connections.
type Conn interface {
	// Read reads data from the connection.
	Read(b []byte) (n int, err error)
	// Write writes data to the connection.
	Write(b []byte) (n int, err error)
	// Close closes the connection.
	Close() error
	// RemoteAddr returns the remote network address.
	RemoteAddr() net.Addr
	// LocalAddr returns the local network address.
	LocalAddr() net.Addr
}
