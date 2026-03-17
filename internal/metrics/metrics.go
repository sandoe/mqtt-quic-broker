// Package metrics provides Prometheus metrics for the MQTT broker.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds all Prometheus metric instruments.
type Metrics struct {
	// Connection metrics
	ConnectedClients    prometheus.Gauge
	TotalConnections    prometheus.Counter
	TotalDisconnections prometheus.Counter
	ConnectLatency      prometheus.Histogram

	// Message metrics
	MessagesPublished  *prometheus.CounterVec
	MessagesDelivered  *prometheus.CounterVec
	MessageThroughput  prometheus.Gauge
	DeliveryLatency    prometheus.Histogram

	// QUIC metrics
	QUICHandshakeTime  prometheus.Histogram
	QUIC0RTTConnections prometheus.Counter

	// Session metrics
	ActiveSessions prometheus.Gauge
}

// New creates and registers all Prometheus metrics.
func New(namespace string) *Metrics {
	m := &Metrics{
		ConnectedClients: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "connected_clients",
			Help:      "Number of currently connected MQTT clients.",
		}),
		TotalConnections: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "total_connections_total",
			Help:      "Total number of client connections accepted.",
		}),
		TotalDisconnections: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "total_disconnections_total",
			Help:      "Total number of client disconnections.",
		}),
		ConnectLatency: promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "connect_latency_seconds",
			Help:      "Latency of CONNECT packet processing.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 12),
		}),
		MessagesPublished: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_published_total",
			Help:      "Total number of messages published, labeled by topic prefix and QoS.",
		}, []string{"qos"}),
		MessagesDelivered: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_delivered_total",
			Help:      "Total number of message deliveries to subscribers.",
		}, []string{"topic_prefix"}),
		MessageThroughput: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "message_throughput_per_second",
			Help:      "Current message throughput (messages/second).",
		}),
		DeliveryLatency: promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "delivery_latency_seconds",
			Help:      "End-to-end message delivery latency.",
			Buckets:   prometheus.ExponentialBuckets(0.00001, 2, 16),
		}),
		QUICHandshakeTime: promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "quic_handshake_duration_seconds",
			Help:      "Duration of QUIC TLS handshake.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 12),
		}),
		QUIC0RTTConnections: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "quic_0rtt_connections_total",
			Help:      "Total number of QUIC 0-RTT connections.",
		}),
		ActiveSessions: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "active_sessions",
			Help:      "Number of active (including disconnected persistent) sessions.",
		}),
	}
	return m
}

// ClientConnected records a new client connection.
func (m *Metrics) ClientConnected() {
	m.ConnectedClients.Inc()
	m.TotalConnections.Inc()
}

// ClientDisconnected records a client disconnection.
func (m *Metrics) ClientDisconnected() {
	m.ConnectedClients.Dec()
	m.TotalDisconnections.Inc()
}

// ObserveConnectLatency records CONNECT processing latency.
func (m *Metrics) ObserveConnectLatency(seconds float64) {
	m.ConnectLatency.Observe(seconds)
}

// MessagePublished records a published message.
func (m *Metrics) MessagePublished(_ string, qos int) {
	m.MessagesPublished.WithLabelValues(formatQoS(qos)).Inc()
}

// MessageDelivered records message deliveries to subscribers.
func (m *Metrics) MessageDelivered(topic string, count int) {
	prefix := topicPrefix(topic)
	m.MessagesDelivered.WithLabelValues(prefix).Add(float64(count))
}

// ObserveQUICHandshake records QUIC handshake duration.
func (m *Metrics) ObserveQUICHandshake(seconds float64) {
	m.QUICHandshakeTime.Observe(seconds)
}

// Record0RTTConnection records a 0-RTT connection.
func (m *Metrics) Record0RTTConnection() {
	m.QUIC0RTTConnections.Inc()
}

// Handler returns the Prometheus HTTP handler.
func Handler() http.Handler {
	return promhttp.Handler()
}

func formatQoS(qos int) string {
	switch qos {
	case 0:
		return "0"
	case 1:
		return "1"
	case 2:
		return "2"
	default:
		return "unknown"
	}
}

func topicPrefix(topic string) string {
	if len(topic) > 20 {
		return topic[:20]
	}
	return topic
}
