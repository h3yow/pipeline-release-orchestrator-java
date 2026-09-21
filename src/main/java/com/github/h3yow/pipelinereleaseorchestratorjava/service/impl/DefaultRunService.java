package com.github.h3yow.pipelinereleaseorchestratorjava.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.h3yow.pipelinereleaseorchestratorjava.config.OrchestratorProperties;
import com.github.h3yow.pipelinereleaseorchestratorjava.exception.IdempotencyConflictException;
import com.github.h3yow.pipelinereleaseorchestratorjava.exception.IllegalTransitionException;
import com.github.h3yow.pipelinereleaseorchestratorjava.exception.OverflowException;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.AuditType;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.DefinitionVersion;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.PipelineDefinition;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.PipelineRun;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.RejectReason;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.RetryPolicy;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.RunStatus;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.StageExecution;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.StageSpec;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.StageStatus;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.DefinitionService;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.Hashes;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.PipelineLockManager;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.RunService;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.stage.StageHandler;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.stage.StageResult;
import com.github.h3yow.pipelinereleaseorchestratorjava.store.LocalStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

@Service
public class DefaultRunService implements RunService {

    private static final Logger log = LoggerFactory.getLogger(DefaultRunService.class);

    private final LocalStateStore store;
    private final PipelineLockManager locks;
    private final DefinitionService definitions;
    private final AuditSupport audit;
    private final StageHandler stageHandler;
    private final OrchestratorProperties properties;
    private final ObjectMapper payloadMapper = new ObjectMapper().findAndRegisterModules();
    private volatile Clock clock = Clock.systemUTC();

    public DefaultRunService(LocalStateStore store, PipelineLockManager locks,
                             DefinitionService definitions, AuditSupport audit,
                             StageHandler stageHandler, OrchestratorProperties properties) {
        this.store = store;
        this.locks = locks;
        this.definitions = definitions;
        this.audit = audit;
        this.stageHandler = stageHandler;
        this.properties = properties;
    }

    /** Replace the clock (tests controlling backoff timing). */
    public void setClock(Clock clock) {
        this.clock = clock;
    }

    @Override
    public PipelineRun submit(String pipelineName, int definitionVersion,
                              String idempotencyKey, Map<String, Object> payload) {
        Objects.requireNonNull(pipelineName, "pipelineName");
        Objects.requireNonNull(idempotencyKey, "idempotencyKey");
        // Validate first so a withdrawn/invalid version is rejected distinctly even on duplicate keys.
        DefinitionVersion dv = definitions.resolveExecutable(pipelineName, definitionVersion);
        String payloadHash = Hashes.sha256(canonicalPayload(payload));

        return locks.withLock(pipelineName, () -> {
            LocalStateStore.IdempotencyRecord existing =
                    store.getIdempotencyRecord(pipelineName, idempotencyKey);
            if (existing != null) {
                PipelineRun existingRun = store.getRun(existing.runId());
                if (!existing.payloadHash().equals(payloadHash)) {
                    audit.emit(AuditType.RUN_REJECTED, pipelineName, definitionVersion,
                            existing.runId(), idempotencyKey, null,
                            existingRun == null ? null : existingRun.getStatus(), null,
                            "same idempotency key submitted with different payload");
                    throw new IdempotencyConflictException(
                            "idempotency key '" + idempotencyKey + "' already bound to run "
                                    + existing.runId() + " with a different payload; "
                                    + "existing run is never overwritten",
                            existing.runId());
                }
                audit.emit(AuditType.RUN_SUBMIT_DEDUP, pipelineName,
                        existingRun == null ? definitionVersion : existingRun.getDefinitionVersion(),
                        existing.runId(), idempotencyKey, null,
                        existingRun == null ? null : existingRun.getStatus(),
                        existingRun == null ? null : existingRun.getStatus(),
                        "duplicate submit returns the original run");
                return existingRun;
            }

            // Resource cap: every submitted run occupies an in-flight slot until terminal.
            // REJECT fails immediately at the cap; QUEUE buffers excess runs (flagged via
            // run.queued) up to a separate bound and drains them in FIFO via pumpQueued().
            long inFlight = store.countInFlight(pipelineName);
            long queuedCount = store.listRuns(pipelineName).stream()
                    .filter(PipelineRun::isQueued).count();
            boolean queue = "QUEUE".equalsIgnoreCase(properties.getOverflowPolicy());
            if (queue) {
                if (inFlight >= properties.getMaxInFlightPerPipeline()
                        && queuedCount >= properties.getMaxQueuedPerPipeline()) {
                    audit.emit(AuditType.OVERFLOW, pipelineName, definitionVersion,
                            null, idempotencyKey, null, null, null,
                            "queue full: inFlight=" + inFlight + " queued=" + queuedCount);
                    throw new OverflowException(RejectReason.OVERFLOW_QUEUE_FULL,
                            "pipeline '" + pipelineName + "' backlog limit reached: queued="
                                    + queuedCount + " maxQueued=" + properties.getMaxQueuedPerPipeline());
                }
            } else if (inFlight >= properties.getMaxInFlightPerPipeline()) {
                audit.emit(AuditType.OVERFLOW, pipelineName, definitionVersion,
                        null, idempotencyKey, null, null, null,
                        "in-flight cap reached: " + inFlight);
                throw new OverflowException(RejectReason.OVERFLOW_REJECTED,
                        "pipeline '" + pipelineName + "' in-flight cap reached: "
                                + inFlight + "/" + properties.getMaxInFlightPerPipeline());
            }

            PipelineRun run = newRun(pipelineName, dv, idempotencyKey, payloadHash);
            run.setQueued(queue && inFlight >= properties.getMaxInFlightPerPipeline());
            run.setStatus(RunStatus.PENDING);
            store.putRun(run);
            store.putIdempotencyIndex(pipelineName, idempotencyKey, run.getRunId(), payloadHash);
            store.persistSnapshot();
            audit.emit(AuditType.RUN_SUBMITTED, pipelineName, definitionVersion,
                    run.getRunId(), idempotencyKey, null, null, RunStatus.PENDING,
                    run.isQueued() ? "queued (in-flight cap)" : "accepted");
            return run;
        });
    }

    @Override
    public PipelineRun advance(String runId) {
        PipelineRun found = mustFindRun(runId);
        return locks.withLock(found.getPipelineName(), () -> {
            PipelineRun run = mustFindRun(runId);
            if (run.getStatus() == RunStatus.PENDING) {
                transition(run, RunStatus.RUNNING, AuditType.RUN_STATUS_CHANGED, "execution started");
            } else if (run.getStatus() == RunStatus.WAITING_RETRY) {
                Instant notBefore = run.getRetryNotBefore();
                if (notBefore != null && Instant.now(clock).isBefore(notBefore)) {
                    throw new IllegalTransitionException(RejectReason.RETRY_NOT_AVAILABLE,
                            "run " + runId + " is in backoff until " + notBefore);
                }
                // The current FAILED stage gets a fresh attempt; reset it so the no-skip
                // invariant holds (only PENDING stages are ever executed).
                StageExecution retrying = currentStage(run);
                if (retrying != null && retrying.getStatus() == StageStatus.FAILED) {
                    retrying.setStatus(StageStatus.PENDING);
                    retrying.setStartedAt(null);
                    retrying.setFinishedAt(null);
                }
                transition(run, RunStatus.RUNNING, AuditType.RUN_RETRIED,
                        "backoff elapsed, retry #" + (run.getRetryAttempts() + 1) + " begins");
            } else if (run.getStatus() == RunStatus.CANCELLING) {
                // cancel() marked intent; the drive loop converges to a terminal state below.
                if (!run.isCancelRequested()) {
                    run.setCancelRequested(true);
                }
            } else if (run.getStatus() != RunStatus.RUNNING) {
                throw new IllegalTransitionException(
                        "run " + runId + " cannot advance from status " + run.getStatus());
            }
            return drive(run);
        });
    }

    @Override
    public PipelineRun retry(String runId) {
        PipelineRun found = mustFindRun(runId);
        return locks.withLock(found.getPipelineName(), () -> {
            PipelineRun run = store.getRun(runId);
            if (run == null) {
                throw new IllegalTransitionException(RejectReason.RUN_NOT_FOUND, "run not found: " + runId);
            }
            if (run.getStatus() == RunStatus.FAILED_NON_RETRYABLE) {
                throw new IllegalTransitionException(RejectReason.NON_RETRYABLE_ERROR,
                        "run " + runId + " failed with a non-retryable error: "
                                + run.getFailureReason());
            }
            if (run.getStatus() != RunStatus.FAILED && run.getStatus() != RunStatus.WAITING_RETRY) {
                throw new IllegalTransitionException(RejectReason.NOT_FAILED,
                        "run " + runId + " is not in a retryable failed state (status="
                                + run.getStatus() + ")");
            }
            RetryPolicy policy = policyOf(run);
            if (run.getRetryAttempts() >= policy.getMaxAttempts()) {
                throw new IllegalTransitionException(RejectReason.RETRY_ATTEMPTS_EXHAUSTED,
                        "run " + runId + " retry attempts exhausted: "
                                + run.getRetryAttempts() + "/" + policy.getMaxAttempts());
            }
            StageExecution stage = currentStage(run);
            if (stage != null && !stage.isLastErrorRetryable()
                    && run.getFailureCode() != RejectReason.RETRY_ATTEMPTS_EXHAUSTED) {
                // Stage-level retryable flag wins: non-retryable stage error terminates immediately.
                throw new IllegalTransitionException(RejectReason.NON_RETRYABLE_ERROR,
                        "stage '" + (stage == null ? "?" : stage.getName())
                                + "' failed with non-retryable error: " + stage.getLastError());
            }
            Duration wait = policy.backoffForAttempt(run.getRetryAttempts() + 1);
            run.setRetryNotBefore(Instant.now(clock).plus(wait));
            transition(run, RunStatus.WAITING_RETRY, AuditType.RUN_RETRY_SCHEDULED,
                    "manual retry scheduled; backoff=" + wait);
            store.persistSnapshot();
            return run;
        });
    }

    @Override
    public PipelineRun cancel(String runId, String reason) {
        PipelineRun found = mustFindRun(runId);
        return locks.withLock(found.getPipelineName(), () -> {
            PipelineRun run = store.getRun(runId);
            if (run == null) {
                throw new IllegalTransitionException(RejectReason.RUN_NOT_FOUND, "run not found: " + runId);
            }
            switch (run.getStatus()) {
                case CANCELLED -> {
                    return run; // idempotent: repeated cancel never flips state
                }
                case SUCCEEDED, FAILED, FAILED_NON_RETRYABLE -> throw new IllegalTransitionException(
                        RejectReason.CANCEL_NOT_ALLOWED,
                        "run " + runId + " is terminal (" + run.getStatus() + ") and cannot be cancelled");
                case CANCELLING -> {
                    run.setCancelRequested(true);
                    return run;
                }
                default -> {
                    run.setCancelRequested(true);
                    if (run.getStatus() == RunStatus.WAITING_RETRY || run.getStatus() == RunStatus.PENDING) {
                        // Nothing is executing: cancellation takes effect immediately and terminally.
                        run.setFinishedAt(Instant.now(clock));
                        transition(run, RunStatus.CANCELLED, AuditType.RUN_CANCELLED, reason);
                    } else {
                        // A stage may be executing under the lock on another thread; mark intent and
                        // let the owning drive() loop converge to CANCELLED (never flip back).
                        transition(run, RunStatus.CANCELLING, AuditType.RUN_CANCEL_REQUESTED, reason);
                    }
                    store.persistSnapshot();
                }
            }
            return run;
        });
    }

    @Override
    public PipelineRun get(String runId) {
        PipelineRun run = store.getRun(runId);
        if (run == null) {
            throw new IllegalTransitionException(RejectReason.RUN_NOT_FOUND, "run not found: " + runId);
        }
        return run;
    }

    @Override
    public List<PipelineRun> pumpQueued(String pipelineName) {
        if (!"QUEUE".equalsIgnoreCase(properties.getOverflowPolicy())) {
            return List.of();
        }
        return locks.withLock(pipelineName, () -> {
            List<PipelineRun> queuedRuns = store.listRuns(pipelineName).stream()
                    .filter(r -> r.isQueued() && r.getStatus() == RunStatus.PENDING)
                    .sorted(Comparator.comparing(PipelineRun::getCreatedAt))
                    .toList();
            long occupying = store.countInFlight(pipelineName) - queuedRuns.size();
            long capacity = properties.getMaxInFlightPerPipeline() - occupying;
            List<PipelineRun> started = new ArrayList<>();
            for (PipelineRun run : queuedRuns) {
                if (capacity <= 0) {
                    break;
                }
                capacity--;
                run.setQueued(false);
                started.add(driveToStarted(run));
            }
            return started;
        });
    }

    private PipelineRun driveToStarted(PipelineRun run) {
        transition(run, RunStatus.RUNNING, AuditType.RUN_STATUS_CHANGED,
                "dequeued by queue pump, execution started");
        return drive(run);
    }

    // ---------------------------------------------------------------- state machine driving

    private PipelineRun drive(PipelineRun run) {
        // A cancel request while idle (PENDING/WAITING_RETRY) takes effect immediately.
        if (run.isCancelRequested() && run.getStatus() != RunStatus.CANCELLING
                && run.getStatus() != RunStatus.RUNNING) {
            run.setFinishedAt(Instant.now(clock));
            transition(run, RunStatus.CANCELLED, AuditType.RUN_CANCELLED, "cancel request observed before execution");
            store.persistSnapshot();
            return run;
        }

        PipelineDefinition def = definitions.get(run.getPipelineName(), run.getDefinitionVersion()).getDefinition();
        while (run.getStatus() == RunStatus.RUNNING || run.getStatus() == RunStatus.CANCELLING) {
            if (run.getCurrentStageIndex() >= def.getStages().size()) {
                finishSuccess(run);
                break;
            }
            StageExecution stage = currentStage(run);

            if (run.getStatus() == RunStatus.CANCELLING || run.isCancelRequested()) {
                // No stage is half-executed here (stage execution is synchronous under the lock),
                // so cancellation cannot interleave with a stage and cannot cause a double advance.
                run.setStatus(RunStatus.CANCELLING);
                run.setFinishedAt(Instant.now(clock));
                transition(run, RunStatus.CANCELLED, AuditType.RUN_CANCELLED,
                        "cancelled at stage '" + stage.getName() + "' (stage not executed)");
                store.persistSnapshot();
                return run;
            }

            // Skip protection: only the immediate next PENDING stage may execute.
            if (stage.getStatus() != StageStatus.PENDING) {
                throw new IllegalTransitionException(RejectReason.STAGE_SKIP_FORBIDDEN,
                        "stage '" + stage.getName() + "' is " + stage.getStatus()
                                + ", only PENDING stages may execute; skipping is forbidden");
            }

            StageSpec spec = def.getStages().get(stage.getIndex());
            stage.setStatus(StageStatus.RUNNING);
            stage.setStartedAt(Instant.now(clock));
            stage.setAttempts(stage.getAttempts() + 1);
            audit.emit(AuditType.RUN_STAGE_STARTED, run.getPipelineName(), run.getDefinitionVersion(),
                    run.getRunId(), run.getIdempotencyKey(), null, run.getStatus(), run.getStatus(),
                    "stage='" + stage.getName() + "' attempt=" + stage.getAttempts());

            StageResult result;
            try {
                result = stageHandler.execute(spec, run, stage.getAttempts());
            } catch (Exception e) {
                result = StageResult.fail(true, String.valueOf(e.getMessage()));
            }
            stage.setFinishedAt(Instant.now(clock));

            if (result != null && result.success()) {
                stage.setStatus(StageStatus.SUCCEEDED);
                stage.setLastError(null);
                audit.emit(AuditType.RUN_STAGE_SUCCEEDED, run.getPipelineName(),
                        run.getDefinitionVersion(), run.getRunId(), run.getIdempotencyKey(), null,
                        run.getStatus(), run.getStatus(), "stage='" + stage.getName() + "'");
                run.setCurrentStageIndex(run.getCurrentStageIndex() + 1);
                store.persistSnapshot();
                // A cancel intent may have been set on this run in a previous tick. Once the
                // in-flight stage is done, converge to CANCELLED before touching the next stage
                // (no duplicate advancement, no status bounce).
                if (run.isCancelRequested() || run.getStatus() == RunStatus.CANCELLING) {
                    run.setFinishedAt(Instant.now(clock));
                    transition(run, RunStatus.CANCELLED, AuditType.RUN_CANCELLED,
                            "cancel observed after stage '" + stage.getName()
                                    + "'; next stage never started");
                    store.persistSnapshot();
                    return run;
                }
                continue;
            }

            // Stage failed: a failed run must never be judged successful.
            boolean retryable = result != null && result.retryable() && spec.retryable();
            stage.setStatus(StageStatus.FAILED);
            stage.setLastError(result == null ? "null result" : result.errorMessage());
            stage.setLastErrorRetryable(retryable);
            audit.emit(AuditType.RUN_STAGE_FAILED, run.getPipelineName(), run.getDefinitionVersion(),
                    run.getRunId(), run.getIdempotencyKey(), null, run.getStatus(), run.getStatus(),
                    "stage='" + stage.getName() + "' retryable=" + retryable
                            + " error=" + stage.getLastError());

            if (!retryable) {
                finishFailed(run, RunStatus.FAILED_NON_RETRYABLE,
                        RejectReason.NON_RETRYABLE_ERROR,
                        "non-retryable failure at stage '" + stage.getName() + "': "
                                + stage.getLastError());
                return run;
            }
            RetryPolicy policy = policyOf(run);
            if (run.getRetryAttempts() >= policy.getMaxAttempts()) {
                finishFailed(run, RunStatus.FAILED, RejectReason.RETRY_ATTEMPTS_EXHAUSTED,
                        "retries exhausted at stage '" + stage.getName() + "': "
                                + stage.getLastError());
                return run;
            }
            Duration wait = policy.backoffForAttempt(run.getRetryAttempts() + 1);
            run.setRetryAttempts(run.getRetryAttempts() + 1);
            run.setRetryNotBefore(Instant.now(clock).plus(wait));
            transition(run, RunStatus.WAITING_RETRY, AuditType.RUN_RETRY_SCHEDULED,
                    "stage='" + stage.getName() + "' backoff=" + wait
                            + " attempts=" + run.getRetryAttempts() + "/" + policy.getMaxAttempts());
            store.persistSnapshot();
            return run;
        }
        return run;
    }

    private void finishSuccess(PipelineRun run) {
        run.setFinishedAt(Instant.now(clock));
        transition(run, RunStatus.SUCCEEDED, AuditType.RUN_SUCCEEDED,
                "all " + run.getStages().size() + " stage(s) succeeded");
        store.persistSnapshot();
    }

    private void finishFailed(PipelineRun run, RunStatus status, RejectReason code, String reason) {
        run.setFinishedAt(Instant.now(clock));
        run.setFailureCode(code);
        run.setFailureReason(reason);
        transition(run, status,
                status == RunStatus.FAILED_NON_RETRYABLE ? AuditType.RUN_FAILED : AuditType.RUN_FAILED,
                code + ": " + reason);
        store.persistSnapshot();
    }

    private void transition(PipelineRun run, RunStatus target, AuditType auditType, String reason) {
        RunStatus from = run.getStatus();
        log.debug("run {} transition {} -> {} ({})", run.getRunId(), from, target, reason);
        run.setStatus(target);
        run.setUpdatedAt(Instant.now(clock));
        audit.emit(auditType, run.getPipelineName(), run.getDefinitionVersion(),
                run.getRunId(), run.getIdempotencyKey(), null, from, target, reason);
    }

    private StageExecution currentStage(PipelineRun run) {
        List<StageExecution> stages = run.getStages();
        int idx = run.getCurrentStageIndex();
        return idx >= 0 && idx < stages.size() ? stages.get(idx) : null;
    }

    private RetryPolicy policyOf(PipelineRun run) {
        DefinitionVersion dv = definitions.get(run.getPipelineName(), run.getDefinitionVersion());
        if (dv == null || dv.getDefinition().getRetryPolicy() == null) {
            return RetryPolicy.noRetry();
        }
        return dv.getDefinition().getRetryPolicy();
    }

    private PipelineRun newRun(String pipelineName, DefinitionVersion dv,
                               String idempotencyKey, String payloadHash) {
        PipelineRun run = new PipelineRun();
        run.setRunId(UUID.randomUUID().toString());
        run.setPipelineName(pipelineName);
        run.setDefinitionVersion(dv.getVersion());
        run.setIdempotencyKey(idempotencyKey);
        run.setPayloadHash(payloadHash);
        run.setStatus(RunStatus.PENDING);
        run.setCurrentStageIndex(0);
        run.setRetryAttempts(0);
        Instant now = Instant.now(clock);
        run.setCreatedAt(now);
        run.setUpdatedAt(now);
        List<StageExecution> executions = new ArrayList<>();
        List<StageSpec> specs = dv.getDefinition().getStages();
        for (int i = 0; i < specs.size(); i++) {
            executions.add(new StageExecution(specs.get(i).name(), i));
        }
        run.setStages(executions);
        run.setPromotedEnvironments(new ArrayList<>());
        return run;
    }

    private PipelineRun mustFindRun(String runId) {
        PipelineRun run = store.getRun(runId);
        if (run == null) {
            throw new IllegalTransitionException(RejectReason.RUN_NOT_FOUND, "run not found: " + runId);
        }
        return run;
    }

    private String canonicalPayload(Map<String, Object> payload) {
        try {
            if (payload == null || payload.isEmpty()) {
                return "{}";
            }
            return payloadMapper.writerWithDefaultPrettyPrinter()
                    .writeValueAsString(new java.util.TreeMap<>(payload));
        } catch (Exception e) {
            // Deterministic last-resort identity so a non-JSON value still yields a stable hash.
            return "raw:" + String.valueOf(payload);
        }
    }
}
