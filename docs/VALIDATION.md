# 验证

两部分：`./mvnw verify` 跑的自动化测试，以及对打包后的 jar 做的真实运行验证（真实进程、真实 HTTP、真实 H2 文件、真实 `kill -9` 和 SIGTERM）。
运行环境：macOS（Apple Silicon）、OpenJDK 21、默认配置（16 个工作线程）。每个脚本的完整输出在 [`docs/validation/`](validation/)。

## 1. 自动化测试（55 个，全部通过）

| 测试类 | 数量 | 证明什么 |
|---|---|---|
| `BoundedPriorityBlockingQueueTest` | 12 | 优先级顺序、同级 FIFO、满时拒绝、带超时阻塞、逐出规则（只逐出更低优先级的最新元素）、老化防饿死、close 唤醒、drain、运行时缩容 |
| `BoundedPriorityBlockingQueueConcurrencyTest` | 4 | 8 生产者 × 5000 + 8 消费者、容量 32、重复 3 轮：无丢失、无重复、峰值 ≤ 容量；并发 `offerOrEvict` 不超限且高压下保留紧急任务 |
| `AdmissionControllerTest` | 6 | 按优先级分级拒绝、自适应上限的上下界、Retry-After 计算、1000 个并发准入在上限 100 时恰好放行 100 个 |
| `RetryPolicyTest` | 2 | 指数增长、封顶、抖动范围 |
| `FailureClassifierTest` | 2 | 四类失败的归类，穿透 `ExecutionException` 包装 |
| `DownstreamCircuitBreakerTest` | 2 | 连续系统故障打开、到期关闭、成功清零 |
| `TaskStoreTest` | 8 | claim 互斥；**失去租约的旧工人写不进结果**；完成时状态、结果、尝试记录同事务；退避未到不可 claim；退还尝试次数；毒丸进死信；按优先级再按时间出队；过期清理连带结果 |
| `TaskLifecycleIntegrationTest` | 13 | 真实 HTTP + H2 文件：接收→执行→取结果；未完成取结果 409；同键同载荷返回原任务（字段顺序无关）；同键不同载荷 409；重试成功；重试用尽进死信并记录原因；不可重试直接失败；代码缺陷进死信；系统故障不占次数；执行超时；重投；参数校验；统计/列表/指标/Prometheus |
| `ConcurrencyIntegrationTest` | 1 | 32 个客户端、1500 个请求（含 500 个重复键）、内存队列只有 8 格：恰好 1000 个任务、每个只执行一次、内存峰值 ≤ 8、溢出任务全部完成 |
| `PriorityOrderIntegrationTest` | 1 | 单工人忙碌时排队的 9 个任务按 HIGH → NORMAL → LOW 的顺序开始执行 |
| `BackpressureIntegrationTest` | 1 | 上限 20：LOW 在 10、NORMAL 在 15、HIGH 在 20 被拒，每次拒绝都是 429 + Retry-After，积压不超上限 |
| `GracefulShutdownRestartTest` | 1 | 2 个执行中 + 8 个排队时停机：执行中的被中断并回写、尝试次数退还；排队的确认在库；限时收敛；同一数据目录重启后 10 个全部完成，被中断过的任务历史为 `RELEASED → SUCCEEDED` |
| `CrashRecoveryIntegrationTest` | 2 | 死进程遗留的 RUNNING 行启动时被恢复（次数用完的进死信）；运行中租约过期的任务被回收并完成 |

## 2. 真实运行验证

脚本都在 [`scripts/`](../scripts)，启动前会检查端口空闲，并在独立的 `run/` 目录下使用全新的数据。

### 2.1 端到端演示 `scripts/demo.sh`

每种情况各提交一个任务。结果：`fail-once` 第 2 次成功；`fail-always` 4 次后进死信，`history` 显示 3 次 RETRY + 1 次 DEAD_LETTER；
`invalid` 1 次即 FAILED；`system-once` 成功且 `attempts=1, systemRetries=1`；`bug` 直接 DEAD_LETTER（SYSTEM_FATAL）；
同键同载荷重复提交返回 200 和原 taskId，同键不同载荷返回 409。完整输出：[`validation/demo.log`](validation/demo.log)。

### 2.2 压测与背压 `scripts/load-test.sh`

64 个并发客户端、5000 个请求（HIGH 20% / NORMAL 40% / LOW 40%）、积压上限 800（关闭自适应，便于观察）：

| 指标 | 结果 |
|---|---|
| 发送速率 | 5000 个请求 4.0 s 发完，约 1250 个/秒 |
| 接收 / 拒绝 | 913 个 202 / 4087 个 429（全部带 Retry-After） |
| 按优先级接收 | HIGH 322、NORMAL 351、LOW 240（LOW 请求量是 HIGH 的 2 倍，接收数反而少 25%） |
| 提交延迟 | p50 20.4 ms、p95 95.3 ms、p99 272.8 ms（含 429 的快速拒绝） |
| 内存队列 | 容量 128，峰值 128，664 个任务溢出到磁盘 |
| 最终 | 913 个接收的任务全部完成（913 个不同 taskId），期间 78 次业务重试、22 次系统重试 |

完整输出：[`validation/load-test.log`](validation/load-test.log)。

### 2.3 `kill -9` 崩溃 `scripts/crash-test.sh`

32 个客户端并发提交 4000 个任务，第 3 秒 `kill -9`，然后在同一数据目录重启：

| 检查 | 结果 |
|---|---|
| 崩溃前收到 202 | 3418 个（另有 582 个请求没有收到响应） |
| 重启后找不到的 202 任务 | **0** |
| 崩溃时正在执行、被恢复的任务 | 15 个（全部重新入队） |
| 客户端用同一批 `bizKey` 全部重试 | 3440 个 200（原 taskId）+ 560 个 202（新任务） |
| 崩溃前已接收、重试后 taskId 变了的 | **0** |
| 最终任务数 | 4000（每个业务键恰好一个）：3999 个 SUCCEEDED，1 个 DEAD_LETTER |

重试时多出的 22 个「200 原 taskId」（3440 − 3418），是崩溃时已经提交、但 202 还没发出去的请求：服务端已接收，客户端重试拿回了同一个任务，没有丢也没有重复。

那 1 个死信是正常的重试用尽：模拟的限流概率是 8%，这个任务（`crash-2169`）4 次尝试全部遇到 429，`task_attempt` 里记录了 3 次 RETRY 和 1 次 DEAD_LETTER，可以人工 redrive。
完整输出：[`validation/crash-test.log`](validation/crash-test.log)。

### 2.4 SIGTERM 优雅停机 `scripts/shutdown-test.sh`

8 个工作线程、每个任务 2–6 s，提交 300 个后 2 s 发 SIGTERM（`await-running=3s`、`await-interrupted=5s`）：

| 检查 | 结果 |
|---|---|
| SIGTERM 时 | 8 个执行中、61 个在内存队列、228 个只在磁盘 |
| 停机期间的新提交 | 503 |
| 引擎停止 | 3024 ms：等 3 s 后中断 1 个仍在执行的任务并回写为 PENDING；61 个内存中的任务全部确认在库 |
| JVM 退出 | 5.3 s（含 Web 服务器和连接池关闭） |
| 重启后 | 300 个全部成功，`recoveredAfterCrash = 0`（没有任务需要崩溃恢复），所有任务 `attempts = 1`（被中断的那次已退还） |

完整输出：[`validation/shutdown-test.log`](validation/shutdown-test.log)。
