package optimistic

import (
	"math/rand"
	"time"
)

// Backoff computes retry delays that grow exponentially with the number of
// consecutive failures, plus random jitter to desynchronize competing writers.
//
// Delay(attempt) = min(Cap, Base * 2^attempt) * (1 ± Jitter*rand)
//
// Jitter is a fraction in [0,1]; 0.5 means the delay varies within ±50%.
// A Backoff value is not goroutine-safe: give each writer its own copy.
type Backoff struct {
	Base       time.Duration // delay for the first retry (attempt 0)
	Cap        time.Duration // hard upper bound for any single delay
	MaxRetries int           // give up after this many failed attempts
	Jitter     float64       // jitter fraction, 0 disables jitter

	rand *rand.Rand
}

// NewBackoff returns a Backoff with its own randomly-seeded source.
func NewBackoff(base, cap time.Duration, maxRetries int, jitter float64) *Backoff {
	return &Backoff{
		Base:       base,
		Cap:        cap,
		MaxRetries: maxRetries,
		Jitter:     jitter,
		rand:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// WithRand replaces the random source (used to make tests deterministic).
func (b *Backoff) WithRand(r *rand.Rand) *Backoff {
	b.rand = r
	return b
}

// Delay returns how long to wait before retry number attempt (0-based).
// The base delay doubles per attempt and is capped at Cap; jitter then
// scales it by a random factor in [1-Jitter, 1+Jitter].
func (b *Backoff) Delay(attempt int) time.Duration {
	d := b.Base
	if attempt > 0 {
		// Guard against shift overflow: any attempt large enough to
		// overflow a duration just clamps to the cap.
		if attempt >= 62 || b.Base > b.Cap {
			d = b.Cap
		} else {
			d = b.Base << uint(attempt)
			if d <= 0 || d > b.Cap {
				d = b.Cap
			}
		}
	}
	if d > b.Cap {
		d = b.Cap
	}
	if b.Jitter > 0 {
		r := b.rand.Float64()
		factor := 1 + b.Jitter*(2*r-1)
		d = time.Duration(float64(d) * factor)
	}
	if d < 0 {
		d = 0
	}
	return d
}
