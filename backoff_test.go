package main

import (
	"testing"
	"time"
)

// 无抖动时延迟必须严格指数增长：base, 2*base, 4*base, ...
func TestDelayExponentialGrowth(t *testing.T) {
	base := 10 * time.Millisecond
	b := NewBackoff(base, time.Hour, 10, 0, 1)
	for attempt := 1; attempt <= 5; attempt++ {
		want := base << (attempt - 1)
		if got := b.Delay(attempt); got != want {
			t.Fatalf("attempt %d: 延迟 %v，期望 %v", attempt, got, want)
		}
	}
}

// 延迟必须被封顶在 Cap。
func TestDelayCapped(t *testing.T) {
	b := NewBackoff(10*time.Millisecond, 50*time.Millisecond, 100, 0, 1)
	for attempt := 4; attempt <= 20; attempt++ {
		if got := b.Delay(attempt); got != 50*time.Millisecond {
			t.Fatalf("attempt %d: 延迟 %v 应被封顶为 50ms", attempt, got)
		}
	}
}

// 带抖动时：延迟落在 [d*(1-jitter), d] 区间内，且样本不全相同（确实随机），
// 同时均值仍随次数指数增长。
func TestDelayJitterBoundsAndRandomness(t *testing.T) {
	base := 10 * time.Millisecond
	jitter := 0.5
	b := NewBackoff(base, time.Hour, 100, jitter, 7)

	const samples = 200
	sum := make([]time.Duration, 4)
	for attempt := 1; attempt <= 4; attempt++ {
		d := base << (attempt - 1)
		lo := time.Duration(float64(d) * (1 - jitter))
		seen := map[time.Duration]bool{}
		for i := 0; i < samples; i++ {
			got := b.Delay(attempt)
			if got < lo || got > d {
				t.Fatalf("attempt %d: 延迟 %v 超出区间 [%v, %v]", attempt, got, lo, d)
			}
			seen[got] = true
			sum[attempt-1] += got
		}
		if len(seen) < 2 {
			t.Fatalf("attempt %d: %d 个样本全部相同，抖动未生效", attempt, samples)
		}
	}
	// 均值应大致翻倍（指数增长趋势）
	for i := 1; i < 4; i++ {
		prev, curv := sum[i-1], sum[i]
		if curv <= prev {
			t.Fatalf("平均延迟未增长: attempt %d 均值 %v，attempt %d 均值 %v",
				i, prev/time.Duration(samples), i+1, curv/time.Duration(samples))
		}
	}
}
