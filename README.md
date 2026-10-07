# optimistic-lock-retry

基于版本号的乐观锁并发更新与自动重试演示系统。仅使用 Go 标准库；
回放页面为单一静态 HTML 文件，原生 HTML/CSS/JavaScript，无任何第三方依赖。

## 运行

```bash
go run .          # 运行两组并发场景，终端输出报告，并生成 replay.html
open replay.html  # 浏览器打开，逐帧回放提交、冲突、重试与活锁检测过程
go test ./...     # 自动化测试
go test -race ./...  # 竞态检测
```

## 一、乐观锁协议（版本号冲突检测）

`Store`（store.go）持有 `(value, version)`，由互斥锁保护。写者遵循
**读-改-写 + 比较并提交** 协议（writer.go 的 `Writer.RunOnce`）：

1. `Read()` 读取当前值 `val` 与版本号 `ver`；
2. 基于 `val` 计算新值（模拟业务计算，可配置 `WorkDelay` 扩大竞争窗口）；
3. `Commit(ver, newVal)`：仅当存储当前版本号仍等于 `ver` 时才写入并把
   版本号 +1；否则拒绝写入并返回 `ErrVersionConflict`。

版本号校验与递增在**同一个临界区**内完成，不存在"校验通过后被插队"的
绕过路径，因此不会丢失更新（lost update）。冲突后写者重新读取最新状态、
重新计算更新、重新提交。正确性判据：最终值 == 成功提交次数（每次提交
恰好 +1），由 `TestConcurrentNoLostUpdates` 在 16 写者 × 25 更新的并发
压力下验证。

## 二、指数退避 + 抖动重试（backoff.go）

第 `attempt` 次重试（从 1 开始）的基础延迟：

```
d = min(Cap, Base * 2^(attempt-1))
sleep = 均匀随机 [ d*(1-Jitter), d ]
```

- **指数增长**：让失败代价随冲突次数上升，降低整体竞争强度；
- **抖动（jitter）**：把延迟随机化，避免多个写者在冲突后于同一时刻
  同步醒来、再次冲突（惊群效应）；
- **上限 Cap**：防止延迟无限增长；
- **MaxRetries**：最大重试次数（不含首次尝试）。超过后写者放弃，
  记录 `giveup` 事件并返回明确错误（"冲突无法解决，N 次重试后放弃"），
  而不是无限重试阻塞。

演示参数：场景 A `base=1ms, cap=30ms, jitter=0.5, maxRetries=12`；
场景 B（活锁）`base=300µs, cap=1.5ms, jitter=0.2, maxRetries=60`。
`TestDelayExponentialGrowth / TestDelayCapped /
TestDelayJitterBoundsAndRandomness / TestGiveUpAfterMaxRetries`
分别覆盖指数增长、封顶、抖动区间与随机性、超限放弃。

## 三、活锁检测与打破（livelock.go）

### 检测依据

活锁区别于死锁：系统仍在运转（事件持续推进），但冲突不断、吞吐趋零。
纯 CAS 系统全局上总有人能提交成功，因此活锁在此**操作化**为两条信号，
任一满足即判定：

1. **单写者连续失败 ≥ FailThreshold**：在带抖动的指数退避下，同一写者
   连续这么多次都恰好撞上他人提交的概率极低（如 32 写者时单次失败
   概率约 31/32，但退避应逐步把写者错开），连续失败次数远超正常退避
   能解释的范围，说明退避机制已失效；
2. **全局连续 ConflictBurst 次冲突而无任何成功提交**：吞吐崩塌的直接
   证据。

一次成功提交复位全部计数并重新武装检测器。

### 打破策略

检测触发后由 `Breaker` 介入：**随机选中一个优先写者（champion）**，
其余写者在一个**有界时间窗口**（`BreakerYield`）内被强制让步
（`livelock_yield` 事件），让优先写者无竞争地完成提交，打破对称性。
窗口过期或任何写者提交成功后自动解除——不会永久阻塞任何写者，
也不会退化为无限让步。

### 场景构造

场景 B 用 32 个写者、起跑栅栏同步开始、800µs 计算窗口、极小的退避
上限（1.5ms）与抖动（0.2），使退避无法把写者错开，冲突持续爆发。
实测触发活锁检测约 28 次，461 次冲突，打破器介入后 64 次更新全部
完成、无丢失更新。`TestLivelockDetectedAndBroken` 验证检测在 15s
内触发且场景最终完成；`TestNormalContentionNoFalsePositive` 验证
正常竞争下不误报。

## 四、回放页面（replay.html）

`go run .` 运行两组场景并把事件流（读取/提交成功/冲突/退避/放弃/
活锁检测/强制让步/完成，带毫秒级时间戳）以 JSON 内嵌进
`replay_template.html`，生成单一静态文件 `replay.html`。浏览器打开后：

- 播放/暂停、0.25×–8× 变速、逐事件步进、时间轴拖动与点击定位；
- 共享资源面板实时显示当前值与版本号；
- 每个写者一张状态卡，随事件切换颜色（读取/冲突/退避/让步/成功/放弃）；
- 时间轴上红色竖线标记活锁检测时刻，触发时页面顶部红色横幅闪烁，
  活锁事件列表可点击跳转；
- 场景报告面板汇总成功/冲突/放弃/活锁次数与丢失更新检查结果。

## 代码结构

| 文件 | 职责 |
|---|---|
| `store.go` | 版本化存储与比较并提交（`Resource` 接口） |
| `backoff.go` | 指数退避 + 抖动策略 |
| `writer.go` | 写者的读-改-写-重试循环 |
| `livelock.go` | 活锁检测器与随机优先让步打破器 |
| `events.go` | 线程安全的回放事件记录 |
| `scenario.go` | 并发场景运行器与统计 |
| `main.go` | 场景配置、报告输出、生成 replay.html |
| `replay_template.html` | 回放页模板（原生 JS，无框架） |
