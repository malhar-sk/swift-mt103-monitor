package swift

import (
	"regexp"
	"testing"
	"time"
)

func resetState() {
	pendingMu.Lock()
	pending = make(map[string]pendingMsg)
	seen = make(map[string]bool)
	pendingMu.Unlock()
	metricMu.Lock()
	msgSentCount, msgDroppedCount, msgDupCount, queueDepthVal, slaBreachCount = 0, 0, 0, 0, 0
	metricMu.Unlock()
}

func TestNewMT103Fields(t *testing.T) {
	known := map[string]bool{}
	for _, b := range banks {
		known[b] = true
	}
	for i := 0; i < 500; i++ {
		m := NewMT103()
		if !regexp.MustCompile(`^TXN\d{1,6}$`).MatchString(m.TxID) {
			t.Fatalf("bad TxID %q", m.TxID)
		}
		if !known[m.Sender] || !known[m.Receiver] {
			t.Fatalf("unknown BIC in %+v", m)
		}
		if m.Amount < 0 || m.Amount >= 10000 {
			t.Fatalf("amount out of range: %v", m.Amount)
		}
		if m.Currency != "USD" {
			t.Fatalf("currency %q", m.Currency)
		}
	}
}

func TestSerializeBlocks(t *testing.T) {
	m := MT103{TxID: "TXN1", Sender: "DEUTDEDB", Receiver: "CHASUS33", Amount: 1234.5, Currency: "USD",
		Timestamp: time.Date(2026, 4, 28, 10, 0, 0, 0, time.UTC)}
	got := m.Serialize()
	want := "{1:F01DEUTDEDB}{2:I103CHASUS33X}{4::32A:260428USD1234.50-}"
	if got != want {
		t.Fatalf("Serialize() = %q, want %q", got, want)
	}
}

func TestAckRemovesPending(t *testing.T) {
	resetState()
	pending["TXN1"] = pendingMsg{txID: "TXN1", sentAt: time.Now()}
	pending["TXN2"] = pendingMsg{txID: "TXN2", sentAt: time.Now()}
	ackMessage("TXN1")
	ackMessage("TXN-unknown") // ignored
	if _, ok := pending["TXN1"]; ok || len(pending) != 1 {
		t.Fatalf("pending after ack = %v", pending)
	}
	if queueDepthVal != 1 {
		t.Fatalf("queue depth = %d, want 1", queueDepthVal)
	}
}

func TestSweepSLACountsOnlyLateMessages(t *testing.T) {
	resetState()
	now := time.Now()
	pending["late"] = pendingMsg{txID: "late", sentAt: now.Add(-6 * time.Second)}
	pending["edge"] = pendingMsg{txID: "edge", sentAt: now.Add(-SLA)} // exactly at the limit: not a breach
	pending["fresh"] = pendingMsg{txID: "fresh", sentAt: now.Add(-1 * time.Second)}
	if n := sweepSLA(now, SLA); n != 1 {
		t.Fatalf("sweepSLA removed %d, want 1", n)
	}
	if slaBreachCount != 1 || msgDroppedCount != 1 || queueDepthVal != 2 {
		t.Fatalf("breaches=%d dropped=%d depth=%d", slaBreachCount, msgDroppedCount, queueDepthVal)
	}
	if _, ok := pending["late"]; ok {
		t.Fatal("late message still pending")
	}
}
