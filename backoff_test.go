package optimistic

import (
	"math/rand"
	"testing"
	"time"
)

// Without jitter, delays must double per attempt and clamp at the cap.
func TestBackoffExponentialGrowth(t *testing.T) {
	b := &Backoff{Base: 10 * time.Millisecond, Cap: 200 * time.Millisecond, Jitter: 0}
	want := []time.Duration{10, 20, 40, 80, 160, 200, 200}
	for attempt, w := range want {
		if got := b.Delay(attempt); got != w*time.Millisecond {
			t.Errorf("Delay(%d) = %v, want %v", attempt, got, w*time.Millisecond)
		}
	}
	// Absurd attempt counts must not overflow — they clamp to the cap.
	if got := b.Delay(1000); got != 200*time.Millisecond {
		t.Errorf("Delay(1000) = %v, want cap 200ms", got)
	}
}

// With jitter, every delay must stay within [d*(1-j), d*(1+j)] of the
// deterministic value d, and samples must actually vary.
func TestBackoffJitterBounds(t *testing.T) {
	const jitter = 0.5
	b := (&Backoff{Base: 100 * time.Millisecond, Cap: 10 * time.Second, Jitter: jitter}).
		WithRand(rand.New(rand.NewSource(1)))
	base := 100 * time.Millisecond
	seen := map[time.Duration]bool{}
	for i := 0; i < 200; i++ {
		got := b.Delay(2) // deterministic part: 400ms
		det := base << 2
		lo := time.Duration(float64(det) * (1 - jitter))
		hi := time.Duration(float64(det) * (1 + jitter))
		if got < lo || got > hi {
			t.Fatalf("Delay with jitter = %v, outside [%v, %v]", got, lo, hi)
		}
		seen[got] = true
	}
	if len(seen) < 50 {
		t.Fatalf("jitter produced only %d distinct delays in 200 samples", len(seen))
	}
}

// Growth must be exponential in expectation even with jitter: the average
// of attempt k+1 samples should clearly exceed attempt k samples.
func TestBackoffAverageGrows(t *testing.T) {
	b := (&Backoff{Base: 10 * time.Millisecond, Cap: time.Minute, Jitter: 0.3}).
		WithRand(rand.New(rand.NewSource(7)))
	avg := func(attempt int) float64 {
		var sum float64
		for i := 0; i < 500; i++ {
			sum += float64(b.Delay(attempt))
		}
		return sum / 500
	}
	if avg(3) <= avg(1)*2 {
		t.Fatalf("average delay did not grow exponentially: avg(1)=%v avg(3)=%v",
			time.Duration(avg(1)), time.Duration(avg(3)))
	}
}
