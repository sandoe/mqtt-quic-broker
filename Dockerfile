# --- Build stage ---
FROM golang:1.24-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w -extldflags '-static'" \
    -o /mqtt-quic-broker ./cmd/broker

# --- Runtime stage ---
FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /mqtt-quic-broker /mqtt-quic-broker
COPY --from=builder /build/configs/ /configs/

# QUIC port (UDP)
EXPOSE 14567/udp
# TCP fallback port
EXPOSE 1883/tcp
# Prometheus metrics
EXPOSE 9090/tcp

ENTRYPOINT ["/mqtt-quic-broker"]
CMD ["--config", "/configs/broker.yaml"]
