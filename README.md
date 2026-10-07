# optimistic-lock-retry

基于版本号的乐观锁并发更新与自动重试（仅 Go 标准库），附带活锁检测与打破策略，
以及一个单文件静态 HTML 回放器（原生 HTML/CSS/JS，无任何框架）。

## 结构

| 文件 | 职责 |
|---|---|
| `store.go` | 版本化资源存储：`Read()` 返回 `(value, version)`，`CompareAndSwap(expectedVersion, newValue)` 仅在版本匹配时写入 |
| `backoff.go` | 指数退避 + 随机抖动的重试间隔计算 |
| `updater.go` | 读-改-写重试循环：冲突 → 重读 → 重算 → 重提交，超限报 `ErrConflictExhausted` |
| `livelock.go` | 活锁检测器与优先级令牌让步机制 |
| `events.go` | 事件时间线（供回放） |
| `simulation.go` | 多写者并发模拟器 |
| `cmd/sim/` | 运行模拟并生成单文件 `replay.html` |

## 一、乐观锁协议（防丢失更新）

1. **读取**：`Read()` 原子返回当前值与版本号 `v`。
2. **计算**：基于读到的值计算新值（纯函数，可安全重跑）。
3. **提交**：`CompareAndSwap(v, newValue)` —— 仅当存储版本仍等于 `v` 时写入并
   将版本号 +1；否则**拒绝写入**，返回当前版本。
4. **冲突重试**：被拒绝后必须重新执行整个循环（重读最新状态、重新计算、重新提交），
   绝不允许跳过版本校验直接覆盖 —— 这是防止 lost update 的唯一关口。

正确性论证：所有写都经过 `CompareAndSwap` 的版本校验，而版本号只在成功写入时
单调递增，因此两次成功写入不可能基于同一个版本 —— 后提交者必然看到版本已变并被
拒绝，转入重试。测试 `TestStoreConcurrentNoLostUpdates`（16 写者 × 50 次递增）
验证最终值与版本号精确等于总提交数。

## 二、退避策略与参数选择

```
Delay(attempt) = min(Cap, Base × 2^attempt) × (1 ± Jitter × rand)
```

- **Base**（默认 500µs）：首次重试间隔。取一次"读-改-写"往返的正常耗时量级，
  太小会让竞争者在同一窗口反复相撞，太大则浪费吞吐。
- **指数增长 ×2**：连续失败说明竞争窗口拥挤，每次翻倍让写者快速错开。
- **Cap**（默认 20ms）：退避上限，防止长尾延迟无限增长；也是防止"退避本身
  造成事实饿死"的保险。
- **Jitter**（默认 0.3，即 ±30%）：关键参数。若所有写者按相同退避表重试，
  它们会在每个周期重新同步相撞（活锁的温床）。抖动把重试时刻打散，
  破坏同步性。`jitter=0` 即"同步重试"，是本仓库构造活锁场景的开关。
- **MaxRetries**（默认 8）：超过后放弃并返回 `ErrConflictExhausted`，
  调用方必须显式处理（上报/入死信队列），绝不无限重试阻塞。

## 三、活锁检测与打破

**特征**：没有死锁（无人持有锁等待），但某些写者持续冲突、长期无法提交。
典型成因：多写者高竞争 + 退避表相同（无抖动或抖动不足）→ 重试周期同步 →
每个周期固定地互相踩踏。

**检测依据**：在带抖动的公平退避下，单个写者连续冲突 k 次的概率随 k 几何衰减
（约 `((W-1)/W)^k`，W 为竞争写者数）。检测器统计**每个写者的连续冲突次数**
（任何一次成功提交即清零），超过阈值 `Threshold`（默认 8~10，远超正常退避
可解释的范围）即判定活锁。分散的、被成功提交打断的冲突永远不会累积成误报
（`TestDetectorCommitResetsStreak` 验证）。

**打破策略**（优先级令牌 + 强制让步）：

1. 检测器触发时，把**优先级令牌**授予被饿死的写者（受害者优先），
   有效期 `PriorityHold`。
2. 令牌有效期内，其他写者冲突后不再走正常退避，而是**强制让步**
   一段随机时长（`[YieldDuration, 2×YieldDuration)`）。
3. 令牌持有者一旦提交成功，剧集结束，全员恢复正常退避；
   令牌过期也会自动释放，避免令牌持有者崩溃导致系统卡死。

这种人为制造的不对称打破了"大家一起退避、一起重试、一起冲突"的对称循环。

## 四、运行模拟并生成回放

```sh
go run ./cmd/sim -out replay.html
# 可选参数：-writers 10 -updates 8 -jitter 0.3 -max-retries 8 \
#           -livelock-threshold 10 -seed 42 （-jitter 0 可构造同步重试活锁）
```

用浏览器打开生成的 `replay.html`（单文件、数据内嵌、无服务、无第三方库）：

- 每个写者一条泳道，色块表示状态：读取 / 提交尝试 / 冲突 / 退避 / 让步 / 提交成功 / 放弃
- 红色竖线标记活锁检测触发时刻，顶部横幅在活锁剧集期间保持亮起
- 播放 / 暂停 / 变速（0.5×–8×）/ 拖动进度条逐帧回放
- 下方事件日志随播放头滚动

## 五、测试

```sh
go test ./...
```

| 测试 | 覆盖点 |
|---|---|
| `TestStoreRejectsStaleVersion` | 过期版本的写入必须被拒绝，不得覆盖 |
| `TestStoreConcurrentNoLostUpdates` | 16×50 并发递增，最终值/版本无丢失更新 |
| `TestBackoffExponentialGrowth` | 退避间隔严格按 2^n 增长并在 Cap 处截断（含溢出防护） |
| `TestBackoffJitterBounds` / `TestBackoffAverageGrows` | 抖动在 ±Jitter 界内、样本确实分散、均值仍指数增长 |
| `TestUpdaterGivesUpAfterMaxRetries` | 永远抢不到的写者在 MaxRetries 后放弃并报 `ErrConflictExhausted` |
| `TestUpdaterRetriesThenCommits` | 冲突后重读-重算-重提交，提交值基于最新状态 |
| `TestDetectorLifecycle` / `TestDetectorCommitResetsStreak` | 阈值触发、优先级授予、强制让步、提交解除、无误报 |
| `TestSimulationDetectsAndBreaksLivelock` | 同步高竞争场景（jitter=0）下检测器在 30s 内触发、产生让步、系统收敛且无丢失更新 |
| `TestSimulationNormalRunNoLostUpdates` | 正常抖动退避下全部更新成功、无放弃 |
