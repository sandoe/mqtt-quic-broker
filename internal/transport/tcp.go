// Package transport provides TCP+TLS transport listener.
package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
)

// TCPConn wraps a net.Conn to implement the Conn interface.
type TCPConn struct {
	net.Conn
}

// TCPListener wraps a net.Listener to implement the Listener interface.
type TCPListener struct {
	ln net.Listener
}

// NewTCPListener creates a new TCP+TLS listener on addr.
// If tlsConfig is nil, the listener accepts plain TCP (not recommended for production).
func NewTCPListener(addr string, tlsConfig *tls.Config) (*TCPListener, error) {
	var ln net.Listener
	var err error

	if tlsConfig != nil {
		ln, err = tls.Listen("tcp", addr, tlsConfig)
	} else {
		ln, err = net.Listen("tcp", addr)
	}
	if err != nil {
		return nil, fmt.Errorf("tcp listen %s: %w", addr, err)
	}

	return &TCPListener{ln: ln}, nil
}

// Accept waits for and returns the next TCP connection.
func (l *TCPListener) Accept(ctx context.Context) (Conn, error) {
	// Use a goroutine to make Accept cancellable
	type result struct {
		conn net.Conn
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		conn, err := l.ln.Accept()
		ch <- result{conn, err}
	}()

	select {
	case <-ctx.Done():
		l.ln.Close()
		return nil, ctx.Err()
	case res := <-ch:
		if res.err != nil {
			return nil, res.err
		}
		return &TCPConn{res.conn}, nil
	}
}

// Addr returns the listener's address.
func (l *TCPListener) Addr() net.Addr {
	return l.ln.Addr()
}

// Close stops the listener.
func (l *TCPListener) Close() error {
	return l.ln.Close()
}
