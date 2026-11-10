# async-task-queue-lossless

Java 开发工程师笔试题「异步任务队列 v1.2」作答：一个**入队不丢失**的异步任务队列。

- 场景：**AI 文档摘要**（调用大模型生成摘要，IO 等待为主），模拟耗时与失败，不对接真实模型
- 技术栈：Java 17、Spring Boot 3.5、Spring Data JPA、H2 文件模式、Micrometer；不引入任何队列中间件
- 队列：基于 `ReentrantLock` + `Condition` 自己实现的**有界、三级优先级、带老化防饿死**的阻塞队列
- 不丢失：**先落盘再入内存**，提交事务成功才返回 202；崩溃恢复、租约 + 版本号防护、优雅停机回写
- 背压：按优先级分级拒绝（429 + Retry-After），积压上限随吞吐自适应；内存满时溢出到磁盘
- 重试：四类失败分别处理，指数退避 + 抖动，重试在磁盘上等待，熔断器，死信 + 人工重投
- 验证：55 个单元/集成/并发测试，外加对打包后 jar 的真实运行验证：压测、`kill -9` 崩溃、SIGTERM 停机

文档：
- [docs/DESIGN.md](docs/DESIGN.md) — 设计说明：场景建模、「不丢失」的边界、容量决策与背压选型、重试与幂等、优雅停机、取舍
- [docs/VALIDATION.md](docs/VALIDATION.md) — 测试清单与真实运行结果
- [docs/AI-CONVERSATION.md](docs/AI-CONVERSATION.md) — 与 AI 的关键对话

## 快速开始

需要 JDK 17 或以上。项目自带 Maven Wrapper，不需要预先安装 Maven。

```bash
./mvnw spring-boot:run
```

或者打包后运行：

```bash
./mvnw -DskipTests package && java -jar target/async-task-queue-lossless-1.0.0.jar
```

服务默认在 `8080` 端口（`PORT=9090 java -jar …` 可改），数据写在 `./data/`，重启保留。
启动时自动执行 [`schema.sql`](src/main/resources/schema.sql) 建表。H2 控制台：`http://localhost:8080/h2-console`
（JDBC URL `jdbc:h2:file:./data/taskqueue`，用户 `sa`，密码为空）。

国内网络访问 Maven Central 慢时，可以用项目里的阿里云镜像配置：`./mvnw -s .mvn/aliyun-settings.xml …`。

## 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/tasks` | 提交任务。202 新任务；200 同 `bizKey` 同载荷（返回原 taskId）；409 同 `bizKey` 不同载荷；429 积压已满；503 正在停机。429/503 带 `Retry-After` |
| GET | `/api/tasks/{id}` | 任务状态、尝试次数、重试次数、失败原因、排队/处理时长。加 `?history=true` 返回每次尝试的记录 |
| GET | `/api/tasks/{id}/result` | 成功后取结果；未完成或失败返回 409 和当前状态 |
| GET | `/api/tasks?status=&page=&size=` | 按状态分页列表 |
| GET | `/api/tasks/stats` | 按状态计数：积压、处理中、重试等待、成功、失败、死信 |
| POST | `/api/tasks/{id}/redrive` | 把 FAILED / DEAD_LETTER 任务重新投递 |
| GET | `/api/metrics` | 队列长度/容量、活跃线程、成功/失败/重试计数、平均与 p95 排队和处理时长、吞吐、准入上限 |
| GET | `/actuator/prometheus` | 同上指标的 Prometheus 格式 |

示例：

```bash
curl -s -X POST localhost:8080/api/tasks -H 'content-type: application/json' -d '{
  "bizKey": "doc-42:summary:v1",
  "priority": "HIGH",
  "payload": {"documentId": "doc-42", "text": "Large language models can summarise long documents ...", "maxSummaryChars": 200}
}'
# {"taskId":"…","bizKey":"doc-42:summary:v1","status":"PENDING","duplicate":false,"statusUrl":"/api/tasks/…","resultUrl":"/api/tasks/…/result"}

curl -s localhost:8080/api/tasks/<taskId>
curl -s localhost:8080/api/tasks/<taskId>/result
curl -s localhost:8080/api/tasks/stats
curl -s localhost:8080/api/metrics
```

`payload.simulate` 可以强制触发某种情况：`fail-once`（第一次限流，重试成功）、`fail-always`（一直限流，最终死信）、
`invalid`（不可重试，直接失败）、`system-once`（一次网关 503，不占重试次数）、`bug`（代码缺陷，直接死信）、`slow`（超时）。
`payload.latencyMs` 可以指定模拟的模型耗时。

## 代码结构

```
src/main/java/io/github/yq808/taskqueue/
├── queue/        BoundedPriorityBlockingQueue  自己实现的有界优先级阻塞队列（不依赖 Spring）
├── engine/       TaskSubmissionService   接收：校验 → 幂等 → 准入 → 落盘 → 入内存
│                 AdmissionController     背压：积压上限、按优先级分级、自适应、Retry-After
│                 TaskDispatcher          内存队列的所有者：快速通道 + 低水位补货
│                 WorkerPool / WorkerSlot 工作线程与只在 handler 内中断的停机机制
│                 TaskProcessor           执行与结果处理：成功 / 重试 / 失败 / 死信 / 回写
│                 RetryPolicy, FailureClassifier, DownstreamCircuitBreaker
│                 TaskEngine              生命周期：启动恢复、维护任务、优雅停机
├── persistence/  TaskStore + Repository  所有状态迁移（带条件 UPDATE + 版本号防护）
├── handler/      TaskHandler 接口与 SummarizeTaskHandler（模拟大模型摘要）
├── metrics/      TaskMetrics             计数器与计时器
└── web/          REST 接口
```

## 测试与验证

```bash
./mvnw verify                  # 55 个测试：单元、并发、端到端（真实 H2 文件）、停机重启、崩溃恢复
scripts/demo.sh                # 每种情况各提交一个任务，打印最终状态、结果、尝试记录和指标
scripts/load-test.sh           # 压测：超过积压上限时的 429 分布、溢出到磁盘、全部完成
scripts/crash-test.sh          # 提交过程中 kill -9，重启后核对没有丢任务、客户端重试幂等
scripts/shutdown-test.sh       # SIGTERM 停机：503、限时收敛、回写、重启后全部完成
```

脚本需要 `curl` 和 `jq`，默认使用 `18080` 端口（`PORT=… scripts/xxx.sh` 可改），运行数据在 `run/` 下。
结果见 [docs/VALIDATION.md](docs/VALIDATION.md)。

## 主要配置

全部在 [`application.yml`](src/main/resources/application.yml) 的 `taskqueue.*` 下，含义见 [`TaskQueueProperties`](src/main/java/io/github/yq808/taskqueue/config/TaskQueueProperties.java)。

| 配置 | 默认 | 说明 |
|---|---|---|
| `worker.threads` | 16 | 工作线程数，按下游模型的并发配额定 |
| `queue.capacity` / `capacity-factor` | 0 / 8 | 内存队列容量，0 表示 `线程数 × 系数` = 128 |
| `queue.low-watermark-ratio` | 0.5 | 低于这个比例时从磁盘补货 |
| `queue.aging-threshold` | 30s | 低优先级等待超过它就优先执行 |
| `admission.max-backlog` / `min-backlog` | 10000 / 1000 | 持久化积压上限的上下界 |
| `admission.adaptive` / `target-max-queue-wait` | true / 10m | 上限 = 吞吐 × 可接受排队时间 |
| `admission.normal-share` / `low-share` | 0.9 / 0.7 | NORMAL、LOW 在积压达到上限的这个比例时开始被拒 |
| `retry.max-attempts` | 4 | 业务可重试失败的最大尝试次数（请求里可用 `maxAttempts` 覆盖，1–10） |
| `retry.initial-backoff` / `multiplier` / `max-backoff` / `jitter` | 1s / 2 / 60s / 0.2 | 指数退避 |
| `retry.max-system-retries` | 10 | 系统可恢复故障的单独重试上限 |
| `execution.timeout` / `lease` | 10s / 60s | 单次执行超时；租约必须大于超时 |
| `shutdown.await-running` / `await-interrupted` | 20s / 5s | 停机两阶段的等待时间 |
| `retention.succeeded-ttl` | 7d | 成功任务及其结果的保留时间 |
