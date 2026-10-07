package main

import (
	"errors"
	"sync"
)

// ErrVersionConflict 表示提交时携带的版本号已被其他写者修改，
// 本次写入被拒绝（乐观锁冲突）。
var ErrVersionConflict = errors.New("optimistic lock: version conflict")

// Resource 是写者依赖的读取 / 比较并提交接口。
// 每次提交必须携带读取时看到的版本号，否则视为冲突。
type Resource interface {
	// Read 返回当前值与当前版本号。
	Read() (value int, version uint64)
	// Commit 仅当 expectedVersion 等于当前版本号时才写入新值，
	// 否则返回 ErrVersionConflict，且不得修改任何状态。
	Commit(expectedVersion uint64, newValue int) (newVersion uint64, err error)
}

// Store 是带版本号的单资源存储，由互斥锁保护，
// 版本号校验与递增在同一个临界区内完成，不存在校验绕过。
type Store struct {
	mu      sync.Mutex
	value   int
	version uint64
}

// NewStore 创建初始值为 initial、版本号为 0 的存储。
func NewStore(initial int) *Store {
	return &Store{value: initial}
}

// Read 返回当前值与版本号。
func (s *Store) Read() (int, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.value, s.version
}

// Commit 实现比较并提交（compare-and-swap 语义）：
// 版本号匹配才写入并递增版本号，否则拒绝并返回当前版本号。
func (s *Store) Commit(expectedVersion uint64, newValue int) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.version != expectedVersion {
		return s.version, ErrVersionConflict
	}
	s.value = newValue
	s.version++
	return s.version, nil
}
