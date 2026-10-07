package optimistic

import (
	"math/rand"
	"sync"
	"time"
)

// Detector identifies livelock: writers that keep conflicting far beyond
// what normal contention explains, so retries never converge.
//
// Signal: per-writer consecutive conflict count. Under fair exponential
// backoff with jitter, a writer's conflicts are roughly geometric — with W
// competing writers the chance of k consecutive conflicts decays like
// ((W-1)/W)^k. A writer reaching Threshold consecutive conflicts (default
// far above what jittered backoff should ever produce) is considered
// starved, and the detector declares livelock.
//
// Mitigation: the starved writer is granted a priority token for
// PriorityHold; every other writer that conflicts while the token is live
// must yield for a randomized YieldDuration instead of its normal backoff.
// That forced asymmetry breaks the synchronized-retry cycle.
type Detector struct {
	Threshold     int           // consecutive conflicts per writer that declare livelock
	YieldDuration time.Duration // base forced-yield wait for non-priority writers
	PriorityHold  time.Duration // how long the priority token stays valid

	mu            sync.Mutex
	fails         map[int]int
	active        bool
	priority      int
	priorityUntil time.Time
	triggers      int
	rand          *rand.Rand
	now           func() time.Time
	onLivelock    func(victim int) // notified once per activation
}

// NewDetector creates a Detector. onLivelock may be nil.
func NewDetector(threshold int, yield, hold time.Duration, onLivelock func(victim int)) *Detector {
	return &Detector{
		Threshold:     threshold,
		YieldDuration: yield,
		PriorityHold:  hold,
		fails:         make(map[int]int),
		priority:      -1,
		rand:          rand.New(rand.NewSource(time.Now().UnixNano())),
		now:           time.Now,
		onLivelock:    onLivelock,
	}
}

// RecordFailure registers one more conflict for writer id and returns true
// if this failure newly triggered livelock detection.
func (d *Detector) RecordFailure(id int) bool {
	d.mu.Lock()
	d.fails[id]++
	fired := false
	if !d.active && d.fails[id] >= d.Threshold {
		d.active = true
		d.priority = id // the starved victim wins the priority token
		d.priorityUntil = d.now().Add(d.PriorityHold)
		d.triggers++
		fired = true
	}
	cb := d.onLivelock
	victim := d.priority
	d.mu.Unlock()
	if fired && cb != nil {
		cb(victim)
	}
	return fired
}

// RecordCommit registers a successful commit by writer id, resetting its
// failure streak. A commit from the priority writer ends the livelock
// episode: progress has resumed.
func (d *Detector) RecordCommit(id int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.fails[id] = 0
	if d.active && id == d.priority {
		d.active = false
		d.priority = -1
	}
}

// ShouldYield reports whether writer id must yield right now (livelock
// mitigation active and someone else holds the token) and for how long.
// The yield duration is jittered in [YieldDuration, 2*YieldDuration).
func (d *Detector) ShouldYield(id int) (time.Duration, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.active {
		return 0, false
	}
	if d.now().After(d.priorityUntil) { // token expired, release everyone
		d.active = false
		d.priority = -1
		return 0, false
	}
	if id == d.priority {
		return 0, false
	}
	return d.YieldDuration + time.Duration(d.rand.Int63n(int64(d.YieldDuration)+1)), true
}

// Triggers returns how many times livelock has been detected.
func (d *Detector) Triggers() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.triggers
}

// Active reports whether livelock mitigation is currently engaged.
func (d *Detector) Active() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.active
}
