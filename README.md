# 本地数据流水线编排与发布服务（Pipeline Release Orchestrator）

基于 Spring Boot 的**进程内（in-process）**流水线编排与发布服务。使用内存状态 + 本地文件存储
（JSON 快照 + 追加式审计日志）模拟流水线定义、运行、环境晋级与版本回滚，重点保证
**并发触发、失败重试、版本回退下行为一致且可审计**。无外部数据库 / 消息中间件依赖。

---

## 1. 总体结构

```
model/      状态机枚举与领域对象（定义版本、运行、阶段、发布、审计事件、原因码）
exception/  可区分的领域异常（统一携带 RejectReason）
store/      LocalStateStore：进程内状态 + 本地快照(state.json)/审计日志(audit.jsonl)
service/    DefinitionService / RunService / ReleaseService / AuditService
            PipelineLockManager：按流水线维度的公平锁，串行化所有状态变更
service/impl 各服务实现 + RunQueuePump（QUEUE 策略定时放行排队运行）
web/        薄 REST 门面 + 统一异常映射（原因码 -> HTTP 状态）
config/     OrchestratorProperties（资源上限、退避、存储、溢出策略）
```

关键设计：

- **每条流水线一把公平锁（`ReentrantLock(fair=true)`）**。同一流水线的提交、推进、重试、
  取消、晋级、回滚全部在该锁内完成，因此同流水线**不可能**出现重复推进、越序执行或部分更新；
  不同流水线互不阻塞。
- **阶段执行在锁内同步完成**。取消与重试即使在另一线程并发发起，也只会在锁边界被串行处理，
  状态不会反复跳变，阶段不会被执行两次。
- 所有状态变更先更新内存对象，再 `persistSnapshot()`（临时文件 + 原子 move），并向
  `audit.jsonl` 追加一条不可变审计事件。

---

## 2. 流水线定义版本管理

- 每次 `createVersion(pipeline, definition)` 生成一个**不可变版本**：
  自增版本号 `v1, v2, ...`、内容 SHA-256 哈希、`ACTIVE` 状态、创建时间。
- 旧版本快照永不被原地修改；新版本是全新对象，历史版本的阶段、环境、重试策略保持不变。
- `withdraw(pipeline, v, reason)` 将版本置为 `WITHDRAWN` 并记录撤回时间与原因。
- **运行必须绑定确定的定义版本**（提交时显式传 `definitionVersion`）。
  执行前经 `resolveExecutable` 校验，非法版本被拒绝并给出**可区分原因码**：

| 情况 | RejectReason | HTTP |
|---|---|---|
| 版本不存在 | `DEFINITION_NOT_FOUND` | 422 |
| 版本已被撤回 | `DEFINITION_WITHDRAWN` | 422 |
| 草稿/非法内容（无阶段、阶段重名、环境重名、无重试策略等） | `DEFINITION_INVALID` | 422 |

---

## 3. 运行提交的幂等语义

`submit(pipeline, definitionVersion, idempotencyKey, payload)`：

- 幂等键作用域为「流水线 + key」。载荷经规范化（key 排序）后计算 SHA-256 `payloadHash`。
- **相同 key + 相同载荷**：返回**同一个运行**（同一 `runId`），不产生第二条运行，
  无论原运行处于 PENDING / RUNNING / 终态，最终结果一致；审计记录 `RUN_SUBMIT_DEDUP`。
- **相同 key + 不同载荷**：抛出 `IDEMPOTENT_KEY_PAYLOAD_MISMATCH`（HTTP 409），
  **绝不静默覆盖**已有运行；原运行、其载荷哈希与状态原样保留并可继续使用。
- 校验定义版本合法性先于幂等判定，因此即使 key 重复，撤回/非法版本仍会被明确拒绝。

---

## 4. 运行状态机（编排状态机）

```
PENDING ──advance──> RUNNING ──全部阶段成功──> SUCCEEDED          (终态)
   │                   │  │
   │ cancel            │  └─阶段可重试失败且仍有次数─> WAITING_RETRY
   │ (空闲)            │                              │ backoff 到期 advance
   ▼                   │  阶段不可重试失败              ▼
CANCELLED             ├──────────────────────> FAILED_NON_RETRYABLE (终态)
(终态)                │  可重试失败且次数耗尽─> FAILED               (终态)
                      │
                      └─cancel(运行中)─> CANCELLING ─在阶段边界收敛─> CANCELLED
WAITING_RETRY ─cancel─> CANCELLED   （退避中取消立即终止）
```

- 阶段严格按定义顺序执行。只有「下一个 `PENDING` 阶段」可执行；
  重试时失败阶段被重置为 `PENDING` 再执行，**跳过阶段**抛 `STAGE_SKIP_FORBIDDEN`。
- **失败绝不会被误判为成功**：阶段失败写 `StageExecution.lastError / lastErrorRetryable`，
  运行进入 `FAILED` 或 `FAILED_NON_RETRYABLE`，只有所有阶段成功才 `SUCCEEDED`。
- 非法状态迁移统一抛 `ILLEGAL_TRANSITION`；对终态运行推进/重试被明确拒绝。

### 失败重试与退避

重试策略在**定义版本**内配置（不可变）：`RetryPolicy{maxAttempts, initialBackoff, multiplier, maxBackoff}`。

- 阶段结果 `StageResult(success, retryable, errorMessage)`；`retryable=false`（或阶段定义为
  不可重试）时**立即终止为 `FAILED_NON_RETRYABLE`，保留原始错误原因**，不再消耗重试。
- 可重试失败且仍有次数：运行进入 `WAITING_RETRY`，记录 `retryNotBefore = now + 退避`，
  退避按指数 `initialBackoff * multiplier^(n-1)` 且不超过 `maxBackoff`。
- 退避窗口内 `advance` 抛 `RETRY_NOT_AVAILABLE`；`retry(runId)` 为手动重试入口，
  次数耗尽抛 `RETRY_ATTEMPTS_EXHAUSTED`。
- 退避时长可通过 `DefaultRunService#setClock(Clock)` 在测试中控制。

### 取消

- `cancel(runId, reason)`：PENDING / WAITING_RETRY 立即 `CANCELLED`；RUNNING 置 `CANCELLING`
  并在最近的阶段边界（阶段同步执行完成后、下一阶段开始前）收敛为 `CANCELLED`。
- **取消幂等**：对已 CANCELLED 重复取消状态不变；对 SUCCEEDED/FAILED 终态取消抛
  `CANCEL_NOT_ALLOWED`，历史不被翻转。
- 取消与重试并发时由同一把锁串行化，最终**必然收敛到单一终态**，不会 CANCELLED↔RUNNING 抖动。

---

## 5. 环境晋级与发布

- 仅 `SUCCEEDED` 运行可 `promote(runId)`；否则 `ENV_NO_ELIGIBLE_RUN`。
- 环境顺序取自定义版本的 `promotionEnvironments`（如 `dev → staging → prod`）。
  第 N 次晋级**只能**到第 N 个环境：
  - 跳过环境（顺序不符）被拒绝；
  - 已到最后一个环境后再次晋级抛 `ENV_ALREADY_PROMOTED`；
  - 运行上记录 `promotedEnvironments`，杜绝重复晋级。
- 每次晋级到某环境生成一条 `ReleaseRecord`：
  - 新发布在该环境置 `ACTIVE`；
  - 该环境原 ACTIVE 发布置 `SUPERSEDED`，记录 `supersededByReleaseId` 与时间。

### 回滚规则

`rollback(pipeline, environment, targetReleaseId, reason)`：

- 回滚**作用于该环境当前仍有效（ACTIVE）的发布**；不存在当前发布时 `RELEASE_NOT_FOUND`。
- 目标必须是同流水线、同环境、仍有效（状态非 `ROLLED_BACK`）的历史发布；
  目标是自己、已 ROLLED_BACK、跨环境 / 不存在均有**独立原因码**
  （`RELEASE_NOT_ACTIVE` / `RELEASE_NOT_FOUND`）。
- 回滚后：当前发布置 `ROLLED_BACK`，写入 `rolledBackAt / rolledBackToReleaseId / rollbackReason`；
  目标发布重新置 `ACTIVE` 并清除其取代标记。
- **历史永不删除**：`SUPERSEDED / ROLLED_BACK / ACTIVE` 行全部保留，
  通过发布时间、取代链、回滚链即可解释完整顺序；审计事件额外记录一次回滚与一次重新生效。

---

## 6. 并发安全与资源上限

`OrchestratorProperties`（前缀 `orchestrator`）：

| 配置 | 含义 | 默认 |
|---|---|---|
| `max-in-flight-per-pipeline` | 每条流水线在途（非终态）运行上限 | 16 |
| `max-queued-per-pipeline` | QUEUE 策略下排队运行上限 | 64 |
| `overflow-policy` | `REJECT` 或 `QUEUE` | REJECT |
| `queue-pump-delay-ms` | QUEUE 策略定时泵的轮询间隔 | 100 |
| `storage-dir` / `persistence-enabled` | 本地快照与审计目录 / 是否落盘 | tmpdir / false |

- **REJECT**：在途数达到上限时新提交立即失败，原因 `OVERFLOW_REJECTED`（HTTP 429）。
- **QUEUE**：超出运行槽的提交进入有界队列（运行 `queued=true` 标记，仍计在途），
  队列再满时抛 `OVERFLOW_QUEUE_FULL`（HTTP 429）。`RunQueuePump` 周期性按 FIFO、
  依剩余容量放行排队运行（同样在流水线锁内）。
- 被拒绝的提交不会污染已有运行或发布历史（不产生任何运行 / 发布 / 索引变更，仅审计一条 OVERFLOW）。

---

## 7. 本地存储与审计

- `state.json`：全量快照，写临时文件后原子替换；启动时自动加载
  （定义、运行、幂等索引、发布、审计序列）。
- `audit.jsonl`：**追加式、不可变** JSON-lines 审计日志，全局自增 `seq`。
- 审计事件类型覆盖：定义创建 / 撤回、运行提交 / 去重 / 拒绝、阶段开始 / 成功 / 失败、
  状态迁移、重试调度 / 执行、取消请求 / 完成、环境晋级、发布 / 取代 / 回滚、溢出。
- 每条事件携带 **pipeline、definitionVersion、runId、idempotencyKey、releaseId、
  from→to 状态、原因**，保证状态流转可追溯。
- 单测日志通过 `pipeline.audit` logger 打印上述字段（见控制台输出）。

---

## 8. REST 摘要

| 方法 & 路径 | 说明 |
|---|---|
| POST `/api/pipelines/{p}/definitions` | 创建不可变定义版本 |
| GET  `/api/pipelines/{p}/definitions[/{v}]` | 查询版本 |
| POST `/api/pipelines/{p}/definitions/{v}/withdraw` | 撤回版本 |
| POST `/api/pipelines/{p}/runs` | 幂等提交运行（body 含 definitionVersion/idempotencyKey/payload） |
| GET  `/api/runs/{runId}` | 查询运行 |
| POST `/api/runs/{runId}/advance` | 推进下一阶段（含退避到期后的重试推进） |
| POST `/api/runs/{runId}/retry` | 手动重试失败运行 |
| POST `/api/runs/{runId}/cancel` | 请求取消 |
| POST `/api/runs/{runId}/promote` | 晋级到下一环境 |
| GET  `/api/pipelines/{p}/releases` | 发布历史（不丢弃） |
| POST `/api/pipelines/{p}/environments/{env}/rollback` | 回滚当前发布到指定历史发布 |
| GET  `/api/pipelines/{p}/audit` | 该流水线审计轨迹 |

错误响应统一为 `{ "reason": "<RejectReason>", "message": "..." }`，
409 = 幂等键载荷冲突，429 = 资源上限，422 = 其它可区分的状态机 / 定义 / 发布拒绝。

---

## 9. 验证方法

需要 JDK 21+ 与 Maven（在 JDK 25 上亦可编译运行）。

```bash
mvn test
```

测试以「重复提交、越序晋级、失败重试、取消、回滚」为主，共 22 个用例：

| 测试类 | 覆盖点 |
|---|---|
| `DefinitionVersioningTests` | 不可变版本、内容哈希、撤回 / 不存在 / 非法原因可区分 |
| `IdempotencyTests` | 同键同载荷唯一运行、同键异载荷拒绝且不覆盖 |
| `PromotionTests` | 环境按序晋级、重复晋级拒绝、失败运行不能发布 |
| `RetryTests` | 可重试成功、退避窗口拒绝、次数耗尽、不可重试立即终止并保留原因 |
| `CancelTests` | PENDING 立即取消、重复取消不跳变、取消与重试并发收敛单终态 |
| `RollbackTests` | 回滚重新生效、完整有序历史、非法目标原因码区分 |
| `ConcurrencyTests` | 16 线程并发推进每阶段仅一次；REJECT 上限拒绝；QUEUE 有界缓冲 + FIFO 泵 |
| `PersistenceTests` | state.json / audit.jsonl 落盘与重载一致性 |
| `WebApiTests` | REST 全链路与 HTTP / 原因码映射 |

单测日志中可见形如：

```
seq=.. type=RUN_STATUS_CHANGED pipeline=svc version=1 runId=.. idempotencyKey=.. PENDING->RUNNING reason=..
type=RUN_STAGE_FAILED ... reason=stage='build' retryable=true error=..
type=RELEASE_ROLLED_BACK releaseId=.. reason=rolled back from v2 to release .. : hotfix regression
```

构建可执行产物：

```bash
mvn spring-boot:run
# 关闭持久化时仅使用进程内状态；设置 orchestrator.persistence-enabled=true 启用本地落盘
```
