package main

import (
	"sync"
	"time"
)

// EventType 描述回放事件类型。
type EventType string

const (
	EvRead             EventType = "read"              // 写者读取了当前值与版本号
	EvCommitOK         EventType = "commit_ok"         // 提交成功，版本号已递增
	EvConflict         EventType = "conflict"          // 提交时版本号不匹配，被拒绝
	EvBackoff          EventType = "backoff"           // 冲突后进入退避等待
	EvGiveUp           EventType = "giveup"            // 超过最大重试次数，放弃
	EvLivelockDetected EventType = "livelock_detected" // 检测到活锁特征
	EvLivelockYield    EventType = "livelock_yield"    // 写者被强制让步以打破活锁
	EvDone             EventType = "done"              // 写者完成全部更新任务
)

// Event 是一条回放事件，时间戳为相对场景开始的毫秒数。
type Event struct {
	T        float64   `json:"t"`                  // 距场景开始的毫秒数
	Writer   int       `json:"writer"`             // 写者 ID，-1 表示系统事件
	Type     EventType `json:"type"`               // 事件类型
	Attempt  int       `json:"attempt,omitempty"`  // 第几次尝试（从 1 开始）
	Version  uint64    `json:"version,omitempty"`  // 相关版本号
	Value    int       `json:"value,omitempty"`    // 相关值
	DelayMs  float64   `json:"delayMs,omitempty"`  // 退避 / 让步时长（毫秒）
	Failures int       `json:"failures,omitempty"` // 触发时的连续失败次数
	Msg      string    `json:"msg,omitempty"`      // 附加说明
}

// Recorder 线程安全地记录事件流，时间戳在锁内获取以保证单调有序。
type Recorder struct {
	mu     sync.Mutex
	start  time.Time
	events []Event
}

// NewRecorder 创建记录器，以当前时刻为零点。
func NewRecorder() *Recorder {
	return &Recorder{start: time.Now()}
}

// Log 追加一条事件，T 字段由记录器自动填充。
func (r *Recorder) Log(e Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e.T = float64(time.Since(r.start).Microseconds()) / 1000.0
	r.events = append(r.events, e)
}

// Events 返回已记录事件的副本。
func (r *Recorder) Events() []Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Event, len(r.events))
	copy(out, r.events)
	return out
}

// msf 将 Duration 转换为毫秒浮点数。
func msf(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000.0
}
