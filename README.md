# Pipeline Release Orchestrator (Java)

本地数据流水线编排与发布服务。使用**进程内状态 + 本地 JSONL 存储**模拟流水线的定义、运行、
环境晋级、失败重试、取消、发布与回滚，保证并发触发、失败重试与版本回退下行为一致、可审计。

## 1. 核心模型

| 模型 | 说明 |
| --- | --- |
| `PipelineDefinition` | 流水线内容：有序阶段 `StageSpec(name, retryable, environments[])` 与有序环境 `environments[]`。不可变。 |
| `DefinitionVersion` | 定义的一个不可变版本，版本号 `v-<内容哈希>`（内容寻址）。内容永不变，生命周期状态只能 `ACTIVE -> WITHDRAWN`。 |
| `RunRecord` | 一次运行，绑定**确定的** `definitionVersion`、`idempotencyKey`、`payloadHash`，持有游标（当前环境/阶段/尝试次数）与失败原因。 |
| `ReleaseRecord` | 发布历史条目（`DEPLOY` / `ROLLBACK`，`ACTIVE` / `SUPERSEDED`），只追加、不删除。 |
| `AuditEvent` | 只追加审计事件，带全局单调递增 `seq`，记录幂等键、定义版本、状态流转明细。 |

所有变更都会向 `<storageDir>/<stream>.jsonl` 追加一行，历史不会被静默改写或丢弃。

## 2. 编排状态机

```
QUEUED ──────────────► RUNNING ──────────────► WAITING_PROMOTION
                          │  ▲                        │
                          │  └──── 自动重试(退避) ◄────┤ promote()
                          │                            ▼
                          │                         RUNNING（下一环境，阶段游标归零）
                          ├──────────────► SUCCEEDED（全部环境完成，先发布再置成功）
                          ├──────────────► FAILED_FATAL      （不可重试错误，立即终止）
                          └─► FAILED_RETRYABLE（自动退避中 / 自动次数耗尽，可手动 retry()）
任何非终态 ───────────► CANCELLED
终态：SUCCEEDED / FAILED_FATAL / CANCELLED（FAILED_RETRYABLE 为可恢复的非终态）
```

规则：

- **阶段严格按定义顺序推进**，只执行声明适用于当前环境的阶段；不允许跳过阶段。
- **环境只能按声明顺序晋级**：只有 `WAITING_PROMOTION` 可以 `promote()`，晋级后进入下一环境并把阶段游标重置为该环境第一个适用阶段。
- **重复晋级、对 RUNNING/QUEUED/CANCELLED/SUCCEEDED 晋级、状态回退一律拒绝**（`IllegalRunStateException`）。
- **失败不会被误判为成功**：只有全部环境阶段成功才 `SUCCEEDED`，且发布动作先于成功事件落盘。
- 运行执行阶段与退避 sleep 均在**流水线锁之外**进行；结果回写时在锁内重新校验状态与游标，
  因此取消可以在阶段执行/退避期间生效，过期结果被直接丢弃，不会出现状态反复跳变或重复推进。

## 3. 定义版本管理

- 版本号由 `(pipelineId, 名称, 环境, 阶段名称/可重试/适用环境)` 的规范化内容做 SHA-256 得到：
  相同内容重复提交返回**同一个版本**；任何内容变更产生新版本。
- 运行必须显式绑定一个存在的版本；版本不存在 / 已撤回会分别得到可区分的错误：
  - `DefinitionNotFoundException`（404）
  - `DefinitionWithdrawnException`（409）
  - 定义非法（无环境/阶段、引用未知环境、阶段重名等）→ `InvalidDefinitionException`（400）
- 撤回后不仅新提交被拒，已入队但尚未执行的运行在 worker 启动时也会因
  `DEFINITION_WITHDRAWN` 立即 `FAILED_FATAL`。

## 4. 幂等语义

- `submitRun(pipelineId, versionId, idempotencyKey, payload)` 对 `(pipelineId, idempotencyKey)` 唯一。
- 相同幂等键 + 相同载荷（SHA-256 比对）：重复提交返回**同一个 run**，最终结果一致，并记录
  `RUN_DEDUPLICATED` 审计事件。
- 相同幂等键 + **不同载荷**：抛 `IdempotencyConflictException`（400），**绝不静默覆盖**已有运行。
- 幂等判断先于容量判断：容量满时同键重试仍能拿回原运行。

## 5. 失败重试与取消

- 重试策略可配置（`orchestrator.default-*`）：`maxAttempts`（含首次）、
  `initialBackoffMillis`、`maxBackoffMillis`，退避为确定性指数退避（×2，无 jitter，便于测试）。
- 两类**不可重试**错误立即 `FAILED_FATAL` 并保留原始消息：
  1. 阶段声明 `retryable=false`；2. 执行结果显式 `fatal=true`。
- 可重试失败：自动重试到 `maxAttempts`；期间为 `FAILED_RETRYABLE` 并在退避后自动恢复。
  全部自动次数耗尽后停留 `FAILED_RETRYABLE + RETRIES_EXHAUSTED`，等待人工 `retry()`。
- `cancel()` 可在任意非终态调用；退避中取消会阻止待执行的自动重试。对终态取消/重试被拒绝。
- 并发取消：同流水线所有流转由每把流水线锁串行化，多次取消恰好一次成功，审计中恰好一条
  `RUN_CANCELLED`，不会重复推进。

## 6. 发布、回滚与审计

- 运行在最后一个环境全部成功时自动发布（`DEPLOY`），并把同环境旧发布标记 `SUPERSEDED`
  （旧行保留，新行追加）。
- **回滚只能作用于仍有效（ACTIVE）且未被取代的发布**：
  - 传入已被取代 / 不存在的 releaseId → `RollbackNotAllowedException`；
  - 当前发布是历史上第一条（无前任可恢复）→ 拒绝；
  - 合法回滚：恢复“紧邻上一条发布”的定义版本（该版本定义必须仍 ACTIVE），
    生成新的 `ROLLBACK` 发布（`relatedReleaseId` 指回当前发布），当前发布被取代。
- 回滚**新增历史、绝不删除**：发布流中每次取代/回滚都可按时间与 releaseId 解释。
- 所有关键动作（定义创建/撤回、提交/去重/拒绝、阶段开始/成功/失败、重试、取消、晋级、
  发布、取代、回滚）都写入审计流，可通过 `/api/audit` 查询，`seq` 严格有序。

## 7. 并发安全与资源上限

- 每条流水线一把独立 monitor；同一流水线的提交、晋级、重试、取消、阶段结果应用全部串行，
  不同流水线可并行。阶段执行在锁外，保证控制操作不会被长任务阻塞。
- 容量策略（`orchestrator.reject-policy`）：
  - `REJECT`：活跃运行达到 `maxConcurrentRuns` 立即拒绝（HTTP 429
    `CapacityRejectedException`），不创建运行、不污染历史。
  - `QUEUE`（默认）：最多接收 `maxConcurrentRuns + queueCapacity` 个在途运行，超出同样拒绝。

## 8. REST 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/pipelines/{pipelineId}/definitions` | 创建/获取不可变版本（同内容返回同版本） |
| GET  | `/api/pipelines/{pipelineId}/definitions[/{versionId}]` | 查询版本 |
| POST | `/api/pipelines/{pipelineId}/definitions/{versionId}/withdraw` | 撤回版本 |
| POST | `/api/pipelines/{pipelineId}/runs` | 提交运行（body: versionId/idempotencyKey/payload） |
| GET  | `/api/pipelines/{pipelineId}/runs[/{runId}]` | 查询运行 |
| POST | `/api/pipelines/{pipelineId}/runs/{runId}/promote` | 晋级到下一环境 |
| POST | `/api/pipelines/{pipelineId}/runs/{runId}/retry` | 手动重试 |
| POST | `/api/pipelines/{pipelineId}/runs/{runId}/cancel` | 取消 |
| GET  | `/api/pipelines/{pipelineId}/releases?environment=` | 发布历史（逻辑视图） |
| POST | `/api/pipelines/{pipelineId}/rollbacks` | 回滚当前有效发布 |
| GET  | `/api/audit?pipelineId=` | 审计事件（顺序号排序） |

错误响应统一为 `{"error": "<异常类名>", "message": "..."}`，不同原因映射 400/404/409/429。

## 9. 配置（`application.properties`）

```properties
orchestrator.storage-dir=data/pipeline
orchestrator.max-concurrent-runs=4
orchestrator.queue-capacity=100
orchestrator.reject-policy=QUEUE        # QUEUE | REJECT
orchestrator.default-max-attempts=3
orchestrator.default-initial-backoff-millis=50
orchestrator.default-max-backoff-millis=5000
```

## 10. 验证方法

```bash
mvn test
```

测试位于 `src/test/.../tests/`，全部使用 `@TempDir` 隔离本地存储，共 28 个用例，重点覆盖：

- `DefinitionVersioningTest`：内容寻址版本、变更产生新版本、撤回/不存在/非法定义可区分拒绝。
- `IdempotencyTest`：同键同载荷仅一次运行且结果一致、同键异载荷冲突拒绝、异键独立。
- `PromotionStateMachineTest`：阶段顺序、跨环境等待晋级、重复/越序/回退晋级拒绝、
  取消后晋级拒绝、失败不误判成功。
- `RetryAndFailureTest`：自动重试成功、不可重试立即终止且保留原始原因、显式 fatal、
  自动次数耗尽后手动重试成功。
- `CancelConcurrencyTest`：阶段执行中取消、退避中取消、终态取消拒绝、并发取消恰好一次流转。
- `RollbackAndAuditTest`：发布与取代、回滚仅作用于有效发布、历史完整有序、审计携带
  幂等键/定义版本/状态流转且顺序号唯一。
- `ConcurrencySerializationTest`：同键 16 线程并发唯一运行、单流水线串行无越序、
  REJECT 与 QUEUE 两种上限策略。
- `FailureIsolationTest`：失败运行不发布、不取代、不污染既有运行与发布历史。

测试日志在每次状态流转时打印 `run / idempotencyKey / version / from -> to / reason`，
例如：

```
submitted run=run-.. key=key-1 version=v-.. QUEUED
transition run=run-.. key=key-1 version=v-.. QUEUED -> RUNNING (...)
failed run=run-.. key=bad version=v-.. RUNNING -> FAILED_FATAL reason=STAGE_FAILED_FATAL message=...
cancelled run=run-.. key=ck2 version=v-.. FAILED_RETRYABLE -> CANCELLED
rollback release rel-.. restores version v-..
```
