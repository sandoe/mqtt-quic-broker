# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2024-03-17

### Added
- Initial implementation of MQTT v5.0 broker over QUIC transport
- Full MQTT v5.0 packet encoding/decoding (CONNECT, CONNACK, PUBLISH, PUBACK, PUBREC, PUBREL, PUBCOMP, SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK, PINGREQ, PINGRESP, DISCONNECT)
- QoS 0, 1, and 2 message delivery flows
- Retained messages with topic-filter matching
- Last Will and Testament (LWT) support
- QUIC transport listener with 0-RTT support via `quic-go`
- TCP+TLS fallback listener for legacy clients
- TLS 1.3 with optional mTLS for industrial device authentication
- Topic trie with `+` (single-level) and `#` (multi-level) wildcard matching
- Session management: clean start and persistent sessions with configurable expiry
- Client keepalive monitoring (1.5x timeout per MQTT spec)
- YAML-based configuration with sensible IIoT defaults
- Prometheus metrics: connections, messages/sec, latency, QUIC handshake times
- Message prioritization for `control/`, `alarm/`, `safety/` topic prefixes
- Simple QUIC MQTT client library (`pkg/client`)
- Example publisher and subscriber
- TLS certificate generation script
- Dockerfile multi-stage build
- Docker Compose configuration
- GitHub Actions CI pipeline with race detector and benchmarks
- Comprehensive unit tests and benchmarks
