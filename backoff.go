package main

import (
	"math/rand"
	"sync"
	"time"
)

// Backoff 实现带随机抖动的指数退避重试策略。
//
// 第 attempt 次重试（从 1 开始）的基础延迟为 Base * 2^(attempt-1)，
// 上限为 Cap；实际睡眠时间在 [d*(1-Jitter), d] 内均匀随机，
// 抖动用于打散多个写者的重试时刻，避免同步重试再次冲突。
type Backoff struct {
	Base       time.Duration // 首次重试的基础延迟
	Cap        time.Duration // 单次延迟上限
	MaxRetries int           // 最大重试次数（不含首次尝试），超过则放弃
	Jitter     float64       // 抖动比例，0..1

	mu  sync.Mutex
	rnd *rand.Rand
}

// NewBackoff 创建退避策略。jitter 会被裁剪到 [0,1]。
func NewBackoff(base, cap time.Duration, maxRetries int, jitter float64, seed int64) *Backoff {
	if jitter < 0 {
		jitter = 0
	}
	if jitter > 1 {
		jitter = 1
	}
	return &Backoff{
		Base:       base,
		Cap:        cap,
		MaxRetries: maxRetries,
		Jitter:     jitter,
		rnd:        rand.New(rand.NewSource(seed)),
	}
}

// baseDelay 返回第 attempt 次重试未加抖动的延迟：Base*2^(attempt-1)，封顶 Cap。
func (b *Backoff) baseDelay(attempt int) time.Duration {
	d := b.Base
	for i := 1; i < attempt; i++ {
		d *= 2
		if d >= b.Cap {
			return b.Cap
		}
	}
	if d > b.Cap {
		d = b.Cap
	}
	return d
}

// Delay 返回第 attempt 次重试的实际延迟（含抖动），可并发调用。
func (b *Backoff) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := b.baseDelay(attempt)
	lo := float64(d) * (1 - b.Jitter)
	b.mu.Lock()
	r := b.rnd.Float64()
	b.mu.Unlock()
	return time.Duration(lo + r*(float64(d)-lo))
}
