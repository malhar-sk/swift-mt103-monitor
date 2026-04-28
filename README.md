# pimon — SOVEREIGN

A Go telemetry agent running on a Raspberry Pi 5, secured over Tailscale, simulating a SWIFT MT103 payment message bus over Kafka and shipping all metrics via OpenTelemetry to a VictoriaMetrics + Grafana observability stack.

## What it does

- Scrapes Pi hardware metrics (CPU temperature, ping latency)
- Simulates SWIFT MT103 cross-border payment messages over Kafka
- Tracks message ACKs, duplicates, queue depth, and SLA breaches (>5s unacknowledged)
- Exports all metrics via OTel gRPC to a collector → VictoriaMetrics pipeline
- Exposes a Prometheus-compatible scrape endpoint on the Tailscale IP
- Runs as a systemd service, auto-restarts on failure
- Entire stack accessible only over Tailscale tailnet (100.114.198.35)

## Stack

| Component | Role |
|---|---|
| Go daemon (pimon) | Metric collection + SWIFT simulation |
| Apache Kafka | MT103 message bus |
| OTel Collector | Metric ingestion |
| VictoriaMetrics | Metric storage |
| Grafana | Dashboards + alerting |
| Tailscale | Network security layer |

## Metrics

| Metric | Type | Description |
|---|---|---|
| pi.cpu.temperature | Gauge | CPU temp in °C |
| pi.ping.latency | Gauge | Ping latency to 1.1.1.1 in ms |
| swift.messages.sent | Counter | Total MT103 messages sent |
| swift.messages.dropped | Counter | Messages dropped (lost ACK) |
| swift.messages.duplicates | Counter | Duplicate message IDs detected |
| swift.queue.depth | Gauge | Messages currently unacknowledged |
| swift.sla.breaches | Counter | Messages unacknowledged beyond 5s SLA |

## Architecture
Pi Hardware → pimon (Go)
├── OTel gRPC → OTel Collector → VictoriaMetrics → Grafana
├── Prometheus /metrics → :9100
└── Kafka Producer/Monitor → swift.mt103 topic
↑ all traffic over Tailscale

## Run

```bash
sudo systemctl start pimon
```

## Alert

Grafana alert fires when `swift.sla.breaches > 0` — routed to `pimon-alerts` contact point.
