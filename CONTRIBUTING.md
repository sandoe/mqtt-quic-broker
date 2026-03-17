# Contributing to mqtt-quic-broker

Thank you for contributing! This document outlines the process and standards.

## Code of Conduct

Be respectful and constructive. We welcome contributors of all backgrounds.

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/YOUR_USERNAME/mqtt-quic-broker`
3. Create a feature branch: `git checkout -b feature/my-feature`
4. Install dependencies: `go mod download`
5. Generate development certs: `bash scripts/generate_certs.sh`

## Development Workflow

```bash
# Run tests
make test

# Run benchmarks
make bench

# Lint
make lint

# Build
make build
```

## Pull Request Process

1. **Write tests** for any new functionality
2. Ensure `make test` passes (with race detector)
3. Run `make lint` — fix any issues
4. Update `CHANGELOG.md` under `[Unreleased]`
5. Open a PR with a clear description of changes and motivation

## Code Style

- Follow standard Go conventions (`gofmt`, `goimports`)
- Use `log/slog` for structured logging — never `fmt.Print` in library code
- Prefer table-driven tests
- Keep functions focused and small
- Document all exported types and functions

## Testing Requirements

- **Unit tests** for all new public functions
- **Table-driven tests** for packet encoding/decoding
- **Benchmark tests** for performance-critical paths (topic matching, packet encoding)
- Race detector must pass: `go test -race ./...`

## MQTT v5.0 Compliance

All changes affecting the MQTT protocol must comply with the
[MQTT v5.0 specification](https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html).
Reference the relevant section in PR descriptions.

## Security

- Never commit TLS certificates or private keys
- Report security vulnerabilities privately via GitHub Security Advisories
- All network input must be validated and bounded

## Commit Messages

Use conventional commits format:
```
feat: add session persistence to disk
fix: correct QoS2 PUBREL handling
test: add wildcard matching edge cases
docs: update configuration reference
perf: reduce allocations in packet decoder
```
