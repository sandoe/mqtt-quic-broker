// Command broker starts the MQTT v5.0 broker with QUIC and TCP transports.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sandoe/mqtt-quic-broker/internal/broker"
	"github.com/sandoe/mqtt-quic-broker/internal/config"
	"github.com/sandoe/mqtt-quic-broker/internal/metrics"
	"github.com/sandoe/mqtt-quic-broker/internal/mqtt"
	"github.com/sandoe/mqtt-quic-broker/internal/transport"
)

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "configs/broker.yaml", "Path to broker configuration file")
	flag.Parse()

	// Load configuration
	cfg, err := config.Load(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	// Set up structured logging
	logLevel := slog.LevelInfo
	switch cfg.Logging.Level {
	case "debug":
		logLevel = slog.LevelDebug
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	}

	var handler slog.Handler
	if cfg.Logging.Format == "json" {
		handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})
	} else {
		handler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})
	}
	logger := slog.New(handler)
	slog.SetDefault(logger)

	logger.Info("starting MQTT v5.0 QUIC broker",
		"quic_addr", cfg.QUIC.ListenAddr,
		"tcp_addr", cfg.TCP.ListenAddr,
		"metrics_addr", cfg.Metrics.ListenAddr,
	)

	// Create metrics
	var m *metrics.Metrics
	if cfg.Metrics.Enabled {
		m = metrics.New("mqtt_broker")
	}

	// Create broker
	b := broker.New(cfg, m, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b.Start(ctx)

	// Load TLS configuration
	tlsConfig, err := loadTLS(cfg)
	if err != nil {
		logger.Error("failed to load TLS config", "error", err)
		os.Exit(1)
	}

	// Start QUIC listener
	if cfg.QUIC.Enabled {
		ql, err := transport.NewQUICListener(cfg.QUIC.ListenAddr, tlsConfig, cfg.QUIC.Allow0RTT)
		if err != nil {
			logger.Error("failed to start QUIC listener", "error", err)
			os.Exit(1)
		}
		logger.Info("QUIC listener started", "addr", ql.Addr())
		go serveListener(ctx, ql, b, logger, "quic", m)
	}

	// Start TCP+TLS listener
	if cfg.TCP.Enabled {
		tl, err := transport.NewTCPListener(cfg.TCP.ListenAddr, tlsConfig)
		if err != nil {
			logger.Error("failed to start TCP listener", "error", err)
			os.Exit(1)
		}
		logger.Info("TCP listener started", "addr", tl.Addr())
		go serveListener(ctx, tl, b, logger, "tcp", m)
	}

	// Start metrics HTTP server
	if cfg.Metrics.Enabled {
		mux := http.NewServeMux()
		mux.Handle("/metrics", metrics.Handler())
		mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
			fmt.Fprintf(w, `{"status":"ok","clients":%d}`, b.ClientCount())
		})
		srv := &http.Server{
			Addr:         cfg.Metrics.ListenAddr,
			Handler:      mux,
			ReadTimeout:  5 * time.Second,
			WriteTimeout: 10 * time.Second,
		}
		go func() {
			logger.Info("metrics server started", "addr", cfg.Metrics.ListenAddr)
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				logger.Error("metrics server error", "error", err)
			}
		}()
	}

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh

	logger.Info("received shutdown signal", "signal", sig)
	cancel()

	// Allow connections to drain
	time.Sleep(2 * time.Second)
	logger.Info("broker stopped")
}

// serveListener accepts connections from a listener and handles them.
func serveListener(ctx context.Context, l transport.Listener, b *broker.Broker, logger *slog.Logger, transport string, m *metrics.Metrics) {
	for {
		conn, err := l.Accept(ctx)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				logger.Error("accept error", "transport", transport, "error", err)
				time.Sleep(50 * time.Millisecond)
				continue
			}
		}

		go handleConn(ctx, conn, b, logger, m)
	}
}

// handleConn handles a single client connection.
func handleConn(ctx context.Context, conn transport.Conn, b *broker.Broker, logger *slog.Logger, m *metrics.Metrics) {
	defer conn.Close()

	remote := conn.RemoteAddr().String()
	logger.Debug("new connection", "remote", remote)

	// Wrap the transport connection as an MQTT connection
	mqttConn := &connAdapter{conn: conn}

	// Create dispatcher with the broker as handler
	dispatcher := mqtt.NewDispatcher(b, logger)

	if err := dispatcher.Serve(mqttConn, conn); err != nil {
		logger.Debug("connection closed", "remote", remote, "error", err)
	}
}

// connAdapter adapts a transport.Conn to the mqtt.Connection interface.
type connAdapter struct {
	conn transport.Conn
}

func (a *connAdapter) SendPacket(pkt mqtt.Packet) error {
	return pkt.Encode(a.conn)
}

func (a *connAdapter) RemoteAddr() string {
	return a.conn.RemoteAddr().String()
}

func (a *connAdapter) Close() error {
	return a.conn.Close()
}

func loadTLS(cfg *config.Config) (*tls.Config, error) {
	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS13,
	}

	if cfg.TLS.CertFile != "" && cfg.TLS.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.TLS.CertFile, cfg.TLS.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load cert/key: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	if cfg.TLS.CAFile != "" && cfg.TLS.MutualTLS {
		caData, err := os.ReadFile(cfg.TLS.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caData) {
			return nil, fmt.Errorf("failed to parse CA cert")
		}
		tlsCfg.ClientCAs = pool
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return tlsCfg, nil
}
