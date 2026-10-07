package main

import (
	"fmt"
	"sync"
	"time"
)

// LivelockDetector 从冲突 / 成功事件流中识别活锁特征。
//
// 活锁的操作化定义（区别于正常竞争）：系统没有死锁（仍有事件在推进），
// 但冲突持续发生且吞吐趋近于零。两条触发信号，任一满足即判定：
//
//  1. 单个写者连续冲突次数达到 FailThreshold —— 在带抖动的指数退避下，
//     同一写者连续这么多次都恰好撞上他人提交的概率极低，
//     说明退避已无法把写者错开（"连续失败次数远超正常退避能解释的范围"）。
//  2. 全局连续 ConflictBurst 次冲突而没有任何一次成功提交 ——
//     吞吐崩塌的直接证据。
//
// 一次成功提交会复位全部计数并重新武装检测器。
type LivelockDetector struct {
	mu            sync.Mutex
	FailThreshold int // 单写者连续失败阈值
	ConflictBurst int // 全局无成功连续冲突阈值

	consecGlobal int
	perWriter    map[int]int
	triggered    bool
	onTrigger    func(reason string)
}

// NewLivelockDetector 创建检测器；onTrigger 在首次判定时被调用（不在锁内）。
func NewLivelockDetector(failThreshold, conflictBurst int, onTrigger func(reason string)) *LivelockDetector {
	return &LivelockDetector{
		FailThreshold: failThreshold,
		ConflictBurst: conflictBurst,
		perWriter:     make(map[int]int),
		onTrigger:     onTrigger,
	}
}

// OnConflict 上报写者 writerID 的一次冲突。
func (d *LivelockDetector) OnConflict(writerID int) {
	d.mu.Lock()
	d.consecGlobal++
	d.perWriter[writerID]++
	fire := false
	reason := ""
	if !d.triggered {
		switch {
		case d.FailThreshold > 0 && d.perWriter[writerID] >= d.FailThreshold:
			reason = fmt.Sprintf("写者 %d 连续 %d 次提交冲突，远超指数退避可解释的范围", writerID, d.perWriter[writerID])
			fire = true
		case d.ConflictBurst > 0 && d.consecGlobal >= d.ConflictBurst:
			reason = fmt.Sprintf("全局连续 %d 次冲突且无任何成功提交，吞吐已崩塌", d.consecGlobal)
			fire = true
		}
		if fire {
			d.triggered = true
		}
	}
	cb := d.onTrigger
	d.mu.Unlock()
	if fire && cb != nil {
		cb(reason)
	}
}

// OnSuccess 上报写者 writerID 的一次成功提交，复位计数并重新武装检测器。
func (d *LivelockDetector) OnSuccess(writerID int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.consecGlobal = 0
	d.perWriter[writerID] = 0
	d.triggered = false
}

// Breaker 是活锁打破器：被触发后随机指定一个"优先写者"（champion），
// 其余写者在一个有界时间窗口内被强制让步（yield），
// 让优先写者无竞争地完成提交，从而打破对称性。
// 窗口过期或任何一次成功提交后自动解除，不会永久阻塞任何写者。
type Breaker struct {
	mu         sync.Mutex
	yield      time.Duration // 让步窗口时长
	champion   int           // 优先写者 ID，-1 表示未激活
	yieldUntil time.Time
	now        func() time.Time // 可注入，便于测试
}

// NewBreaker 创建打破器，yield 为每次触发时的让步窗口时长。
func NewBreaker(yield time.Duration) *Breaker {
	return &Breaker{yield: yield, champion: -1, now: time.Now}
}

// Trigger 激活打破器，champion 为被随机选中的优先写者。
func (b *Breaker) Trigger(champion int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.champion = champion
	b.yieldUntil = b.now().Add(b.yield)
}

// ShouldYield 判断写者 id 当前是否应让步；返回剩余让步时长。
func (b *Breaker) ShouldYield(id int) (time.Duration, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.champion < 0 || id == b.champion {
		return 0, false
	}
	rem := b.yieldUntil.Sub(b.now())
	if rem <= 0 {
		return 0, false
	}
	return rem, true
}

// Clear 解除打破器（任何写者提交成功后调用）。
func (b *Breaker) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.champion = -1
	b.yieldUntil = time.Time{}
}
