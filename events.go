package optimistic

import (
	"sort"
	"sync"
	"time"
)

// EventType classifies the timeline events recorded during a simulation.
type EventType string

const (
	EventRead     EventType = "read"     // writer read (value, version)
	EventAttempt  EventType = "attempt"  // writer tries to commit at Version
	EventConflict EventType = "conflict" // commit rejected: version moved
	EventBackoff  EventType = "backoff"  // writer waits Detail duration before retry
	EventCommit   EventType = "commit"   // commit succeeded, store now at Version
	EventLivelock EventType = "livelock" // detector fired: starvation pattern found
	EventYield    EventType = "yield"    // writer forced to yield to priority writer
	EventGiveUp   EventType = "give_up"  // retries exhausted, update abandoned
)

// Event is one entry in the replay timeline. T is relative to simulation start.
type Event struct {
	T       time.Duration `json:"t"`
	Writer  int           `json:"writer"`
	Type    EventType     `json:"type"`
	Version uint64        `json:"version"`
	Attempt int           `json:"attempt"`
	Detail  string        `json:"detail,omitempty"`
}

// EventLog is a goroutine-safe, time-ordered record of simulation events.
type EventLog struct {
	mu     sync.Mutex
	start  time.Time
	events []Event
}

func NewEventLog() *EventLog {
	return &EventLog{start: time.Now()}
}

// Add appends an event stamped with the time since the log was created.
func (l *EventLog) Add(e Event) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e.T = time.Since(l.start)
	l.events = append(l.events, e)
}

// Snapshot returns all events sorted by timestamp (stable for equal times).
func (l *EventLog) Snapshot() []Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Event, len(l.events))
	copy(out, l.events)
	sort.SliceStable(out, func(i, j int) bool { return out[i].T < out[j].T })
	return out
}
