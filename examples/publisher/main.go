// Command publisher is an example MQTT publisher using QUIC transport.
package main

import (
"context"
"crypto/tls"
"crypto/x509"
"flag"
"fmt"
"os"
"time"

"github.com/sandoe/mqtt-quic-broker/internal/mqtt"
"github.com/sandoe/mqtt-quic-broker/pkg/client"
)

func main() {
var (
broker  = flag.String("broker", "localhost:14567", "Broker address")
caFile  = flag.String("ca", "", "CA certificate file (empty = use system roots)")
)
flag.Parse()

ctx := context.Background()

tlsCfg, err := buildTLS(*caFile)
if err != nil {
fmt.Fprintf(os.Stderr, "tls: %v\n", err)
os.Exit(1)
}

c, err := client.Connect(ctx, client.Options{
BrokerAddr: *broker,
ClientID:   "example-publisher",
CleanStart: true,
TLSConfig:  tlsCfg,
})
if err != nil {
fmt.Fprintf(os.Stderr, "connect: %v\n", err)
os.Exit(1)
}
defer c.Disconnect()

fmt.Println("Connected to broker")

ticker := time.NewTicker(time.Second)
defer ticker.Stop()

seq := 0
for {
select {
case <-ctx.Done():
return
case t := <-ticker.C:
topic := "sensors/temperature/device-001"
payload := fmt.Sprintf(`{"ts":%d,"value":%.2f,"seq":%d}`,
t.UnixMilli(), 22.5+float64(seq%10)*0.1, seq)

if err := c.Publish(ctx, topic, []byte(payload), mqtt.QoS1); err != nil {
fmt.Fprintf(os.Stderr, "publish: %v\n", err)
continue
}

fmt.Printf("Published: %s -> %s\n", topic, payload)
seq++

if seq >= 10 {
fmt.Println("Done publishing 10 messages")
return
}
}
}
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
