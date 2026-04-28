package swift

import (
    "context"
    "fmt"
    "sync"
    "time"

    "github.com/segmentio/kafka-go"
    otelmetric "go.opentelemetry.io/otel/metric"
)

var (
    msgSentCount   int64
    msgDroppedCount int64
    msgDupCount    int64
    queueDepthVal  int64
    slaBreachCount int64
    metricMu       sync.Mutex
)

func InitMetrics(meter otelmetric.Meter) {
    meter.Int64ObservableCounter("swift.messages.sent",
        otelmetric.WithDescription("Total MT103 messages sent"),
        otelmetric.WithInt64Callback(func(_ context.Context, o otelmetric.Int64Observer) error {
            metricMu.Lock()
            defer metricMu.Unlock()
            o.Observe(msgSentCount)
            return nil
        }),
    )
    meter.Int64ObservableCounter("swift.messages.dropped",
        otelmetric.WithDescription("Total MT103 messages dropped"),
        otelmetric.WithInt64Callback(func(_ context.Context, o otelmetric.Int64Observer) error {
            metricMu.Lock()
            defer metricMu.Unlock()
            o.Observe(msgDroppedCount)
            return nil
        }),
    )
    meter.Int64ObservableCounter("swift.messages.duplicates",
        otelmetric.WithDescription("Total duplicate MT103 messages"),
        otelmetric.WithInt64Callback(func(_ context.Context, o otelmetric.Int64Observer) error {
            metricMu.Lock()
            defer metricMu.Unlock()
            o.Observe(msgDupCount)
            return nil
        }),
    )
    meter.Int64ObservableGauge("swift.queue.depth",
        otelmetric.WithDescription("Unacknowledged messages in flight"),
        otelmetric.WithInt64Callback(func(_ context.Context, o otelmetric.Int64Observer) error {
            metricMu.Lock()
            defer metricMu.Unlock()
            o.Observe(queueDepthVal)
            return nil
        }),
    )
    meter.Int64ObservableCounter("swift.sla.breaches",
        otelmetric.WithDescription("Messages unacknowledged beyond 5s SLA"),
        otelmetric.WithInt64Callback(func(_ context.Context, o otelmetric.Int64Observer) error {
            metricMu.Lock()
            defer metricMu.Unlock()
            o.Observe(slaBreachCount)
            return nil
        }),
    )
}

type pendingMsg struct {
    txID   string
    sentAt time.Time
}

var (
    pending   = make(map[string]pendingMsg)
    pendingMu sync.Mutex
    seen      = make(map[string]bool)
)

func ackMessage(txID string) {
    pendingMu.Lock()
    defer pendingMu.Unlock()
    if _, ok := pending[txID]; ok {
        delete(pending, txID)
        metricMu.Lock()
        queueDepthVal = int64(len(pending))
        metricMu.Unlock()
        fmt.Printf("[ACK] %s\n", txID)
    }
}

func StartMonitor(ctx context.Context, brokerAddr string) {
    r := kafka.NewReader(kafka.ReaderConfig{
        Brokers: []string{brokerAddr},
        Topic:   "swift.mt103",
        GroupID: "pimon-monitor",
    })
    defer r.Close()

    // SLA watchdog
    go func() {
        for {
            time.Sleep(1 * time.Second)
            pendingMu.Lock()
            for id, msg := range pending {
                if time.Since(msg.sentAt) > 5*time.Second {
                    fmt.Printf("[SLA BREACH] %s unacked for >5s\n", id)
                    metricMu.Lock()
                    slaBreachCount++
                    msgDroppedCount++
                    metricMu.Unlock()
                    delete(pending, id)
                    metricMu.Lock()
                    queueDepthVal = int64(len(pending))
                    metricMu.Unlock()
                }
            }
            pendingMu.Unlock()
        }
    }()

    // Simulate ACK delay — messages sit in pending for 1-3s before ACK
    go func() {
        for {
            time.Sleep(1 * time.Second)
            pendingMu.Lock()
            toAck := []string{}
            for id, msg := range pending {
                if time.Since(msg.sentAt) > 1*time.Second {
                    toAck = append(toAck, id)
                }
            }
            pendingMu.Unlock()
            for _, id := range toAck {
                ackMessage(id)
            }
        }
    }()

    for {
        m, err := r.ReadMessage(ctx)
        if err != nil {
            return
        }
        txID := string(m.Key)

        pendingMu.Lock()
        if seen[txID] {
            fmt.Printf("[DUPLICATE] %s\n", txID)
            metricMu.Lock()
            msgDupCount++
            metricMu.Unlock()
        } else {
            seen[txID] = true
            pending[txID] = pendingMsg{txID: txID, sentAt: time.Now()}
            metricMu.Lock()
            msgSentCount++
            queueDepthVal = int64(len(pending))
            metricMu.Unlock()
            fmt.Printf("[QUEUED] %s\n", txID)
        }
        pendingMu.Unlock()
    }
}
