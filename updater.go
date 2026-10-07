package optimistic

import (
	"errors"
	"time"
)

// ErrConflictExhausted is returned when an update could not be committed
// within Backoff.MaxRetries attempts. The caller must treat the update as
// lost and surface it, never silently drop it.
var ErrConflictExhausted = errors.New("optimistic: max retries exhausted, conflict unresolved")

// Updater runs read-modify-write cycles against a Store with optimistic
// version checks, exponential-backoff retries and livelock mitigation.
type Updater struct {
	ID      int
	Store   *Store
	Backoff *Backoff
	Det     *Detector // may be nil (no livelock handling)
	Log     *EventLog // may be nil (no recording)

	// sleep is replaceable so tests can run without real waiting.
	sleep func(time.Duration)
}

// NewUpdater builds an Updater; log and det may be nil.
func NewUpdater(id int, store *Store, backoff *Backoff, det *Detector, log *EventLog) *Updater {
	return &Updater{ID: id, Store: store, Backoff: backoff, Det: det, Log: log, sleep: time.Sleep}
}

// SetSleep overrides the sleep function (for tests).
func (u *Updater) SetSleep(f func(time.Duration)) { u.sleep = f }

func (u *Updater) emit(t EventType, version uint64, attempt int, detail string) {
	if u.Log != nil {
		u.Log.Add(Event{Writer: u.ID, Type: t, Version: version, Attempt: attempt, Detail: detail})
	}
}

// Update applies fn to the current store value and commits it, retrying on
// version conflicts. fn must be pure: it is re-run on every retry against
// the freshly re-read value. Returns the committed version, or
// ErrConflictExhausted after MaxRetries failed attempts.
func (u *Updater) Update(fn func(old int64) int64) (uint64, error) {
	for attempt := 0; ; attempt++ {
		val, ver := u.Store.Read()
		u.emit(EventRead, ver, attempt, "")

		newVal := fn(val) // recompute on every retry against fresh state

		u.emit(EventAttempt, ver, attempt, "")
		newVer, ok := u.Store.CompareAndSwap(ver, newVal)
		if ok {
			if u.Det != nil {
				u.Det.RecordCommit(u.ID)
			}
			u.emit(EventCommit, newVer, attempt, "")
			return newVer, nil
		}

		u.emit(EventConflict, ver, attempt, "")
		if u.Det != nil {
			u.Det.RecordFailure(u.ID)
		}

		if attempt >= u.Backoff.MaxRetries {
			u.emit(EventGiveUp, ver, attempt, "")
			return 0, ErrConflictExhausted
		}

		// Livelock mitigation: if the detector is engaged and we do not
		// hold the priority token, yield instead of normal backoff.
		if u.Det != nil {
			if d, yield := u.Det.ShouldYield(u.ID); yield {
				u.emit(EventYield, ver, attempt, d.String())
				u.sleep(d)
				continue
			}
		}

		d := u.Backoff.Delay(attempt)
		u.emit(EventBackoff, ver, attempt, d.String())
		u.sleep(d)
	}
}
