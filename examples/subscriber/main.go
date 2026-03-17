// Command subscriber is an example MQTT subscriber using QUIC transport.
package main

import (
"context"
"crypto/tls"
"crypto/x509"
"flag"
"fmt"
"os"
"os/signal"
"syscall"

"github.com/sandoe/mqtt-quic-broker/internal/mqtt"
"github.com/sandoe/mqtt-quic-broker/pkg/client"
)

func main() {
var (
broker = flag.String("broker", "localhost:14567", "Broker address")
caFile = flag.String("ca", "", "CA certificate file (empty = use system roots)")
)
flag.Parse()

ctx, cancel := context.WithCancel(context.Background())
defer cancel()

tlsCfg, err := buildTLS(*caFile)
if err != nil {
fmt.Fprintf(os.Stderr, "tls: %v\n", err)
os.Exit(1)
}

c, err := client.Connect(ctx, client.Options{
BrokerAddr: *broker,
ClientID:   "example-subscriber",
CleanStart: true,
TLSConfig:  tlsCfg,
OnMessage: func(topic string, payload []byte, qos mqtt.QoS) {
fmt.Printf("[%s] QoS%d: %s\n", topic, qos, string(payload))
},
})
if err != nil {
fmt.Fprintf(os.Stderr, "connect: %v\n", err)
os.Exit(1)
}
defer c.Disconnect()

fmt.Println("Connected to broker")

filters := []string{
"sensors/#",
"alarm/#",
"control/#",
}

for _, f := range filters {
if err := c.Subscribe(ctx, f, mqtt.QoS1); err != nil {
fmt.Fprintf(os.Stderr, "subscribe %s: %v\n", f, err)
} else {
fmt.Printf("Subscribed to: %s\n", f)
}
}

sigCh := make(chan os.Signal, 1)
signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
<-sigCh
fmt.Println("\nDisconnecting...")
}

func buildTLS(caFile string) (*tls.Config, error) {
cfg := &tls.Config{MinVersion: tls.VersionTLS13}
if caFile == "" {
return cfg, nil
}
caPEM, err := os.ReadFile(caFile)
if err != nil {
return nil, fmt.Errorf("read CA: %w", err)
}
pool := x509.NewCertPool()
if !pool.AppendCertsFromPEM(caPEM) {
return nil, fmt.Errorf("failed to parse CA cert")
}
cfg.RootCAs = pool
return cfg, nil
}
