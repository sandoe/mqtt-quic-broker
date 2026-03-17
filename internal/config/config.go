// Package config provides YAML configuration loading for the MQTT broker.
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config holds all broker configuration.
type Config struct {
	Broker    BrokerConfig    `yaml:"broker"`
	QUIC      QUICConfig      `yaml:"quic"`
	TCP       TCPConfig       `yaml:"tcp"`
	TLS       TLSConfig       `yaml:"tls"`
	Auth      AuthConfig      `yaml:"auth"`
	Metrics   MetricsConfig   `yaml:"metrics"`
	Logging   LoggingConfig   `yaml:"logging"`
}

// BrokerConfig holds core broker settings.
type BrokerConfig struct {
	MaxClients         int      `yaml:"max_clients"`
	MaxMessageSize     int      `yaml:"max_message_size"`
	MaxQueuedMessages  int      `yaml:"max_queued_messages"`
	SessionExpiry      int      `yaml:"session_expiry_seconds"`
	PriorityTopics     []string `yaml:"priority_topics"`
	RateLimitMsgSec    int      `yaml:"rate_limit_msg_sec"`
	RateLimitBurst     int      `yaml:"rate_limit_burst"`
	TopicAliasMaximum  int      `yaml:"topic_alias_maximum"`
	ReceiveMaximum     int      `yaml:"receive_maximum"`
}

// QUICConfig holds QUIC transport settings.
type QUICConfig struct {
	Enabled    bool   `yaml:"enabled"`
	ListenAddr string `yaml:"listen_addr"`
	Allow0RTT  bool   `yaml:"allow_0rtt"`
}

// TCPConfig holds TCP transport settings.
type TCPConfig struct {
	Enabled    bool   `yaml:"enabled"`
	ListenAddr string `yaml:"listen_addr"`
}

// TLSConfig holds TLS settings.
type TLSConfig struct {
	CertFile   string `yaml:"cert_file"`
	KeyFile    string `yaml:"key_file"`
	CAFile     string `yaml:"ca_file"`
	MutualTLS  bool   `yaml:"mutual_tls"`
}

// AuthConfig holds authentication settings.
type AuthConfig struct {
	Enabled     bool              `yaml:"enabled"`
	AllowAnon   bool              `yaml:"allow_anonymous"`
	Credentials map[string]string `yaml:"credentials"`
}

// MetricsConfig holds Prometheus metrics settings.
type MetricsConfig struct {
	Enabled    bool   `yaml:"enabled"`
	ListenAddr string `yaml:"listen_addr"`
}

// LoggingConfig holds logging settings.
type LoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

// Default returns a Config with sensible IIoT defaults.
func Default() *Config {
	return &Config{
		Broker: BrokerConfig{
			MaxClients:        10000,
			MaxMessageSize:    268435455,
			MaxQueuedMessages: 1000,
			SessionExpiry:     3600,
			PriorityTopics: []string{
				"control/",
				"alarm/",
				"safety/",
			},
			RateLimitMsgSec:   10000,
			RateLimitBurst:    1000,
			TopicAliasMaximum: 65535,
			ReceiveMaximum:    65535,
		},
		QUIC: QUICConfig{
			Enabled:    true,
			ListenAddr: ":14567",
			Allow0RTT:  true,
		},
		TCP: TCPConfig{
			Enabled:    true,
			ListenAddr: ":1883",
		},
		TLS: TLSConfig{
			CertFile:  "certs/server.crt",
			KeyFile:   "certs/server.key",
			CAFile:    "certs/ca.crt",
			MutualTLS: false,
		},
		Auth: AuthConfig{
			Enabled:   false,
			AllowAnon: true,
		},
		Metrics: MetricsConfig{
			Enabled:    true,
			ListenAddr: ":9090",
		},
		Logging: LoggingConfig{
			Level:  "info",
			Format: "json",
		},
	}
}

// Load reads configuration from the given YAML file path.
// If path is empty, returns Default().
func Load(path string) (*Config, error) {
	cfg := Default()
	if path == "" {
		return cfg, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}

	return cfg, nil
}
