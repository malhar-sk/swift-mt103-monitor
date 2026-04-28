package swift

import (
    "context"
    "fmt"
    "math/rand"
    "time"

    "github.com/segmentio/kafka-go"
)

type MT103 struct {
    TxID      string
    Sender    string
    Receiver  string
    Amount    float64
    Currency  string
    Timestamp time.Time
}

var banks = []string{"DEUTDEDB", "CHASUS33", "BARCGB22", "BNPAFRPP", "HSBCHKHH"}

func NewMT103() MT103 {
    return MT103{
        TxID:      fmt.Sprintf("TXN%d", rand.Intn(999999)),
        Sender:    banks[rand.Intn(len(banks))],
        Receiver:  banks[rand.Intn(len(banks))],
        Amount:    float64(rand.Intn(1000000)) / 100.0,
        Currency:  "USD",
        Timestamp: time.Now(),
    }
}

func (m MT103) Serialize() string {
    return fmt.Sprintf("{1:F01%s}{2:I103%sX}{4::32A:%s%s%.2f-}",
        m.Sender, m.Receiver,
        m.Timestamp.Format("060102"),
        m.Currency, m.Amount)
}

func StartProducer(ctx context.Context, brokerAddr string, failRate float64) {
    w := &kafka.Writer{
        Addr:     kafka.TCP(brokerAddr),
        Topic:    "swift.mt103",
        Balancer: &kafka.LeastBytes{},
    }
    defer w.Close()

    for {
        select {
        case <-ctx.Done():
            return
        default:
            msg := NewMT103()
            // simulate message loss
            if rand.Float64() < failRate {
                fmt.Printf("[DROPPED] %s\n", msg.TxID)
                time.Sleep(500 * time.Millisecond)
                continue
            }
            err := w.WriteMessages(ctx, kafka.Message{
                Key:   []byte(msg.TxID),
                Value: []byte(msg.Serialize()),
            })
            if err == nil {
                fmt.Printf("[SENT] %s %.2f %s\n", msg.TxID, msg.Amount, msg.Currency)
            }
            time.Sleep(500 * time.Millisecond)
        }
    }
}
