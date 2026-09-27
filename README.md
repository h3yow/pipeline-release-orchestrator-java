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
  **撤回会立即作用于在途运行**：状态翻转是 lock-free 的（`volatile`），正在执行该版本的
  推进循环会在当前阶段结束后、下一阶段开始前观察到撤回并停止继续执行该版本；
  已受理但未结束（排队中 / 退避等待中 / 执行中）的运行以 `INVALIDATED` 终态收敛
  （详见 §4.2）。
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

任意非终态 ─abort──> ABORTED            (终态；打断在途阶段，见 §4.1)
任意非终态 ─绑定版本被撤回──> INVALIDATED  (终态；区别于失败与取消，见 §4.2)
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

### 4.1 立即中止（abort）

`abort(runId, reason)` 用于「运行跑到一半必须马上叫停」的场景，语义强于 `cancel`：

- **尽快打断在途阶段**：中止意图（`abortRequested` + 原因）在获取流水线锁**之前**就以
  `volatile` 方式写到运行对象上，并 `interrupt()` 正在执行阶段的线程；推进循环在阶段
  结束后、下一阶段开始前检查该标志，立即收敛到 `ABORTED`，**不再执行任何后续阶段**。
  若阶段处理器响应中断（如可中断的等待/IO），长阶段会被当场打断；否则在当前阶段
  完成后、下一阶段开始前停止。
- **阶段结果如实保留**：中止前已跑完的阶段保持 `SUCCEEDED`；被打断的阶段标记为
  `StageStatus.ABORTED`（绝不记为成功）；从未执行的阶段保持 `PENDING`，不留成功痕迹。
- **唯一终态**：运行收敛到终态 `ABORTED`，`failureCode=RUN_ABORTED`，
  `failureReason` 记录触发原因；运行详情（`GET /api/runs/{runId}`）可直接读出，
  完整状态流转顺序由审计事件（`RUN_ABORT_REQUESTED` / `RUN_STAGE_ABORTED` /
  `RUN_ABORTED`，各带 from→to 状态）还原。
- **幂等与拒绝**：重复中止不改变结果（仍 `ABORTED`，`finishedAt` 不变）；对已进入
  其它终态（SUCCEEDED/FAILED/CANCELLED/INVALIDATED）的运行中止抛
  `ABORT_NOT_ALLOWED`（HTTP 422），与 `CANCEL_NOT_ALLOWED` 可区分，且不会在已结束的
  运行上留下中止痕迹。
- **与取消的关系**：中止优先于取消——并发发起时若中止先生效，运行收敛 `ABORTED`，
  随后的取消按「终态不可取消」拒绝；反之取消先收敛则中止按 `ABORT_NOT_ALLOWED` 拒绝。
  两者都由同一把流水线锁串行化，不会出现状态反复或半更新。

### 4.2 定义版本运行中途失效（withdraw → INVALIDATED）

版本撤回不再只影响新提交，而是立即作用于**已受理但尚未结束**的运行：

- 绑定该版本且处于排队中（`PENDING`+queued）、退避等待中（`WAITING_RETRY`）或
  执行中（`RUNNING`）的运行，以终态 `INVALIDATED` 收敛，
  `failureCode=DEFINITION_WITHDRAWN`，`failureReason` 含撤回原因——
  与普通失败（`FAILED*`/`NON_RETRYABLE_ERROR`）、普通取消（`CANCELLED`）完全可区分。
- **后续不得再执行该版本**：推进循环在每个阶段边界检查绑定版本状态；版本一旦被撤回，
  当前阶段如实完成后即停止，不再开始下一阶段；退避中的运行不再被推进，
  `retry()` 对绑定已撤回版本的运行抛 `DEFINITION_WITHDRAWN`。
- **历史保留**：此前已完成阶段的记录（状态、时间、错误）原样保留；运行不会被删除。
- **隔离性**：同流水线的其它版本、其它运行、已有发布历史（含已 SUPERSEDED/ROLLED_BACK
  记录）完全不受影响；已 `SUCCEEDED` 的运行保持原判，其发布仍可参与回滚。
- 已在收敛途中的取消/中止优先：撤回不会把正在走向 `CANCELLED`/`ABORTED` 的运行
  改判为 `INVALIDATED`。

### 4.3 中断与容量、幂等的协同

- `ABORTED` / `INVALIDATED` 都是终态，**立即释放在途槽位**；排队运行被终止时同时清除
  `queued` 标记，**立即释放排队额度**，后续提交按既有 `max-in-flight` / `max-queued`
  上限正常受理（取消路径同样修复为取消即释放排队额度）。
- 幂等键索引在中断前后保持稳定：同一 key 的重复提交仍只对应同一次运行。
  版本未被撤回时重复提交返回原运行（含其中断后的终态）；版本已撤回时按
  `DEFINITION_WITHDRAWN` 明确拒绝（校验先于幂等判定），绝不创建新运行。
- `ABORTED` / `INVALIDATED` 运行不可晋级（仅 `SUCCEEDED` 可 `promote`），
  因此对发布与回滚链路零影响。

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
  状态迁移、重试调度 / 执行、取消请求 / 完成、中止请求 / 阶段被打断 / 中止完成、
  版本失效终止（RUN_INVALIDATED）、环境晋级、发布 / 取代 / 回滚、溢出。
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
| POST `/api/runs/{runId}/abort` | 立即中止（打断在途阶段，收敛 ABORTED） |
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

测试以「重复提交、越序晋级、失败重试、取消、立即中止、版本失效、回滚」为主，共 33 个用例：

| 测试类 | 覆盖点 |
|---|---|
| `DefinitionVersioningTests` | 不可变版本、内容哈希、撤回 / 不存在 / 非法原因可区分 |
| `IdempotencyTests` | 同键同载荷唯一运行、同键异载荷拒绝且不覆盖 |
| `PromotionTests` | 环境按序晋级、重复晋级拒绝、失败运行不能发布 |
| `RetryTests` | 可重试成功、退避窗口拒绝、次数耗尽、不可重试立即终止并保留原因 |
| `CancelTests` | PENDING 立即取消、重复取消不跳变、取消与重试并发收敛单终态 |
| `AbortTests` | 执行中打断在途阶段且后续阶段不执行、PENDING/WAITING_RETRY 立即中止、重复中止幂等、终态中止抛 ABORT_NOT_ALLOWED、中止释放在途容量、中止与取消并发收敛单终态 |
| `WithdrawInvalidationTests` | 撤回作用于排队中/退避中/执行中运行（INVALIDATED 且保留已完成阶段）、排队额度释放、撤回后重试/提交被拒绝、其它版本与发布历史隔离、幂等键跨中断稳定 |
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
