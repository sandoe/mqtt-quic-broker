// Package transport provides QUIC transport listener.
package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"github.com/quic-go/quic-go"
)

// QUICConn wraps a QUIC stream to implement the Conn interface.
type QUICConn struct {
	stream *quic.Stream
	conn   *quic.Conn
	closed bool
}

// Read implements Conn.
func (c *QUICConn) Read(b []byte) (int, error) {
	return c.stream.Read(b)
}

// Write implements Conn.
func (c *QUICConn) Write(b []byte) (int, error) {
	return c.stream.Write(b)
}

// Close closes the stream and the underlying connection.
func (c *QUICConn) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	c.stream.Close()
	return c.conn.CloseWithError(0, "connection closed")
}

// RemoteAddr returns the remote address.
func (c *QUICConn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// LocalAddr returns the local address.
func (c *QUICConn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

// QUICListener wraps a quic.Listener to implement the Listener interface.
// When allow0RTT is true, an EarlyListener is used for 0-RTT support.
type QUICListener struct {
	listener     *quic.Listener
	earlyListener *quic.EarlyListener
}

// NewQUICListener creates a new QUIC listener on addr with the given TLS config.
// Set allow0RTT=true to enable QUIC 0-RTT connection resumption.
func NewQUICListener(addr string, tlsConfig *tls.Config, allow0RTT bool) (*QUICListener, error) {
	quicConf := &quic.Config{
		MaxIdleTimeout:       90 * time.Second,
		MaxIncomingStreams:    1000,
		MaxIncomingUniStreams: 1000,
		KeepAlivePeriod:      30 * time.Second,
		EnableDatagrams:      true,
	}

	// Configure ALPN for MQTT over QUIC
	tlsCfg := tlsConfig.Clone()
	if len(tlsCfg.NextProtos) == 0 {
		tlsCfg.NextProtos = []string{"mqtt"}
	}

	if allow0RTT {
		el, err := quic.ListenAddrEarly(addr, tlsCfg, quicConf)
		if err != nil {
			return nil, fmt.Errorf("quic early listen %s: %w", addr, err)
		}
		return &QUICListener{earlyListener: el}, nil
	}

	l, err := quic.ListenAddr(addr, tlsCfg, quicConf)
	if err != nil {
		return nil, fmt.Errorf("quic listen %s: %w", addr, err)
	}
	return &QUICListener{listener: l}, nil
}

// Accept waits for and returns the next QUIC connection's first stream.
func (l *QUICListener) Accept(ctx context.Context) (Conn, error) {
	var conn *quic.Conn
	var err error

	if l.earlyListener != nil {
		conn, err = l.earlyListener.Accept(ctx)
	} else {
		conn, err = l.listener.Accept(ctx)
	}
	if err != nil {
		return nil, err
	}

	// Accept the first bidirectional stream (used for MQTT traffic)
	stream, err := conn.AcceptStream(ctx)
	if err != nil {
		conn.CloseWithError(0, "stream accept failed")
		return nil, err
	}

	return &QUICConn{stream: stream, conn: conn}, nil
}

// Addr returns the listener's address.
func (l *QUICListener) Addr() net.Addr {
	if l.earlyListener != nil {
		return l.earlyListener.Addr()
	}
	return l.listener.Addr()
}

// Close stops the listener.
func (l *QUICListener) Close() error {
	if l.earlyListener != nil {
		return l.earlyListener.Close()
	}
	return l.listener.Close()
}
