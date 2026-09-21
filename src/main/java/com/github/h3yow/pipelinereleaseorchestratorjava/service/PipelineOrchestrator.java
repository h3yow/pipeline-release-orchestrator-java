package com.github.h3yow.pipelinereleaseorchestratorjava.service;

import com.github.h3yow.pipelinereleaseorchestratorjava.config.OrchestratorProperties;
import com.github.h3yow.pipelinereleaseorchestratorjava.error.CapacityRejectedException;
import com.github.h3yow.pipelinereleaseorchestratorjava.error.DefinitionWithdrawnException;
import com.github.h3yow.pipelinereleaseorchestratorjava.error.IllegalRunStateException;
import com.github.h3yow.pipelinereleaseorchestratorjava.error.RunNotFoundException;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.DefinitionStatus;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.DefinitionVersion;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.EventType;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.FailureReason;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.PipelineDefinition;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.RejectPolicy;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.RunRecord;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.RunStatus;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.StageExecution;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.StageSpec;
import com.github.h3yow.pipelinereleaseorchestratorjava.store.AuditLog;
import com.github.h3yow.pipelinereleaseorchestratorjava.store.RunRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Core orchestration facade.
 *
 * <p>Concurrency model: every state transition of runs belonging to one pipeline is serialized by
 * a per-pipeline monitor. Stage work runs while holding the monitor (simulated stages are fast);
 * only retry backoff sleeps happen outside it, and cancellation/auto-retry races are resolved when
 * the worker reacquires the monitor, so a run can never bounce between states or advance twice.
 *
 * <p>Run state machine:
 * <pre>
 *   QUEUED -> RUNNING -> WAITING_PROMOTION -> RUNNING -> ... -> SUCCEEDED (publishes release)
 *                     \-> FAILED_RETRYABLE (sleep/backoff -> RUNNING, or exhausted -> FAILED_RETRYABLE)
 *                     \-> FAILED_FATAL (non-retryable stage/error or withdrawn definition)
 *   any non-terminal -> CANCELLED
 * </pre>
 */
public class PipelineOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(PipelineOrchestrator.class);

    private final DefinitionService definitionService;
    private final RunRepository runRepository;
    private final ReleaseService releaseService;
    private final AuditLog auditLog;
    private final StageExecutor stageExecutor;
    private final RetryBackoff retryBackoff;
    private final OrchestratorProperties properties;
    private final com.github.h3yow.pipelinereleaseorchestratorjava.model.RetryPolicy retryPolicy;

    private final ConcurrentMap<String, Object> pipelineLocks = new ConcurrentHashMap<>();
    // run ids whose worker is sleeping and will retry automatically; guarded by the pipeline lock
    private final Set<String> pendingAutoRetry = new HashSet<>();

    private final ThreadPoolExecutor pool;

    public PipelineOrchestrator(DefinitionService definitionService, RunRepository runRepository,
                                ReleaseService releaseService, AuditLog auditLog,
                                StageExecutor stageExecutor, RetryBackoff retryBackoff,
                                OrchestratorProperties properties) {
        this.definitionService = definitionService;
        this.runRepository = runRepository;
        this.releaseService = releaseService;
        this.auditLog = auditLog;
        this.stageExecutor = stageExecutor;
        this.retryBackoff = retryBackoff;
        this.properties = properties;
        this.retryPolicy = new com.github.h3yow.pipelinereleaseorchestratorjava.model.RetryPolicy(
                properties.getDefaultMaxAttempts(),
                properties.getDefaultInitialBackoffMillis(),
                properties.getDefaultMaxBackoffMillis(),
                2.0d);
        int threads = Math.max(1, properties.getMaxConcurrentRuns());
        int queueCapacity = Math.max(1, properties.getQueueCapacity());
        this.pool = new ThreadPoolExecutor(threads, threads, 30L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(queueCapacity),
                r -> {
                    Thread t = new Thread(r, "pipeline-worker");
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.AbortPolicy());
    }

    private Object lockFor(String pipelineId) {
        return pipelineLocks.computeIfAbsent(pipelineId, k -> new Object());
    }

    // ------------------------------------------------------------------ submit

    public RunRecord submitRun(String pipelineId, String versionId, String idempotencyKey, String payload) {
        DefinitionVersion version = definitionService.getVersion(pipelineId, versionId);
        if (version.status() == DefinitionStatus.WITHDRAWN) {
            auditLog.append(EventType.SUBMISSION_REJECTED, pipelineId, null, idempotencyKey, versionId,
                    null, null, "submission rejected: definition version withdrawn");
            throw new DefinitionWithdrawnException(
                    "definition version " + versionId + " is withdrawn and cannot be executed");
        }
        String payloadHash = Idempotency.sha256(payload);
        synchronized (lockFor(pipelineId)) {
            if (idempotencyKey != null) {
                RunRecord existing =
                        runRepository.findByIdempotencyKey(pipelineId, idempotencyKey).orElse(null);
                if (existing != null) {
                    if (!existing.payloadHash().equals(payloadHash)) {
                        auditLog.append(EventType.SUBMISSION_REJECTED, pipelineId, existing.runId(),
                                idempotencyKey, versionId, existing.status(), existing.status(),
                                "idempotency key reused with different payload");
                        throw new com.github.h3yow.pipelinereleaseorchestratorjava.error
                                .IdempotencyConflictException(
                                "idempotency key '" + idempotencyKey + "' is already bound to run "
                                        + existing.runId() + " with a different payload");
                    }
                    auditLog.append(EventType.RUN_DEDUPLICATED, pipelineId, existing.runId(),
                            idempotencyKey, existing.definitionVersion(), existing.status(),
                            existing.status(), "duplicate submission returns the existing run");
                    log.info("deduplicated submission key={} run={} version={} status={}",
                            idempotencyKey, existing.runId(), existing.definitionVersion(),
                            existing.status());
                    return existing;
                }
            }

            if (!hasCapacity()) {
                auditLog.append(EventType.SUBMISSION_REJECTED, pipelineId, null, idempotencyKey,
                        versionId, null, RunStatus.QUEUED,
                        "submission rejected by capacity policy " + properties.getRejectPolicy());
                throw new CapacityRejectedException(
                        "concurrency/backlog limit reached (" + inFlight() + "/" + capacityLimit()
                                + "), policy=" + properties.getRejectPolicy());
            }

            String runId = "run-" + UUID.randomUUID();
            String firstEnv = version.definition().environments().get(0);
            RunRecord run = new RunRecord(runId, pipelineId, idempotencyKey, versionId, payloadHash,
                    Instant.now(), RunStatus.QUEUED, firstEnv, 0, 1, FailureReason.NONE, null);
            runRepository.save(run);
            auditLog.append(EventType.RUN_SUBMITTED, pipelineId, runId, idempotencyKey, versionId,
                    null, RunStatus.QUEUED, "run submitted");
            log.info("submitted run={} key={} version={} QUEUED", runId, idempotencyKey, versionId);
            schedule(run);
            return run;
        }
    }

    // ----------------------------------------------------------------- promote

    public RunRecord promote(String runId) {
        RunRecord run = requireRun(runId);
        synchronized (lockFor(run.pipelineId())) {
            run = requireRun(runId);
            if (run.status() != RunStatus.WAITING_PROMOTION) {
                throw new IllegalRunStateException(
                        "promotion rejected for run " + runId + ": status is " + run.status()
                                + " (must be WAITING_PROMOTION); skipping stages, duplicate promotion "
                                + "and state rollback are not allowed");
            }
            PipelineDefinition definition =
                    definitionService.getVersion(run.pipelineId(), run.definitionVersion()).definition();
            List<String> envs = definition.environments();
            int envIndex = envs.indexOf(run.currentEnvironment());
            if (envIndex < 0 || envIndex + 1 >= envs.size()) {
                throw new IllegalRunStateException(
                        "promotion rejected: no further environment after " + run.currentEnvironment());
            }
            if (!hasCapacity()) {
                throw new CapacityRejectedException("no capacity to continue promoted run " + runId);
            }
            RunStatus from = run.status();
            run.currentEnvironment(envs.get(envIndex + 1));
            run.currentStageIndex(0);
            run.attempt(1);
            run.status(RunStatus.RUNNING);
            runRepository.save(run);
            auditLog.append(EventType.PROMOTED, run.pipelineId(), runId, run.idempotencyKey(),
                    run.definitionVersion(), from, RunStatus.RUNNING,
                    "promoted to environment " + run.currentEnvironment());
            log.info("promoted run={} key={} version={} {} -> RUNNING env={}", runId,
                    run.idempotencyKey(), run.definitionVersion(), from, run.currentEnvironment());
            schedule(run);
            return run;
        }
    }

    // ------------------------------------------------------------------- retry

    public RunRecord retry(String runId) {
        RunRecord run = requireRun(runId);
        synchronized (lockFor(run.pipelineId())) {
            run = requireRun(runId);
            if (run.status() != RunStatus.FAILED_RETRYABLE) {
                throw new IllegalRunStateException(
                        "retry rejected for run " + runId + ": status is " + run.status()
                                + " (only FAILED_RETRYABLE runs can be retried)");
            }
            if (pendingAutoRetry.contains(runId)) {
                throw new IllegalRunStateException(
                        "retry rejected for run " + runId
                                + ": an automatic retry is already scheduled after backoff");
            }
            if (!hasCapacity()) {
                throw new CapacityRejectedException("no capacity to retry run " + runId);
            }
            RunStatus from = run.status();
            run.attempt(1);
            run.failureReason(FailureReason.NONE);
            run.failureMessage(null);
            run.status(RunStatus.RUNNING);
            runRepository.save(run);
            auditLog.append(EventType.RUN_RETRY, run.pipelineId(), runId, run.idempotencyKey(),
                    run.definitionVersion(), from, RunStatus.RUNNING, "manual retry from stage index "
                            + run.currentStageIndex() + " in " + run.currentEnvironment());
            log.info("manual retry run={} key={} version={} {} -> RUNNING", runId,
                    run.idempotencyKey(), run.definitionVersion(), from);
            schedule(run);
            return run;
        }
    }

    // ------------------------------------------------------------------ cancel

    public RunRecord cancel(String runId) {
        RunRecord run = requireRun(runId);
        synchronized (lockFor(run.pipelineId())) {
            run = requireRun(runId);
            if (isTerminal(run.status())) {
                throw new IllegalRunStateException(
                        "cancel rejected for run " + runId + ": run already terminated with "
                                + run.status() + "/" + run.failureReason());
            }
            RunStatus from = run.status();
            run.status(RunStatus.CANCELLED);
            run.failureReason(FailureReason.CANCELLED);
            run.failureMessage("cancelled by request");
            runRepository.save(run);
            auditLog.append(EventType.RUN_CANCELLED, run.pipelineId(), runId, run.idempotencyKey(),
                    run.definitionVersion(), from, RunStatus.CANCELLED, "run cancelled");
            log.info("cancelled run={} key={} version={} {} -> CANCELLED", runId,
                    run.idempotencyKey(), run.definitionVersion(), from);
            return run;
        }
    }

    // ------------------------------------------------------------------ reads

    public RunRecord getRun(String runId) {
        return requireRun(runId);
    }

    public List<RunRecord> runs(String pipelineId) {
        return runRepository.findByPipeline(pipelineId);
    }

    // ------------------------------------------------------------- worker side

    /** Package-private entry point used by {@link RunWorker}. */
    void executeQueuedRun(RunRecord scheduled) {
        RunRecord run = runRepository.findById(scheduled.runId())
                .orElse(scheduled);
        String pipelineId = run.pipelineId();
        Object lock = lockFor(pipelineId);
        synchronized (lock) {
            if (run.status() == RunStatus.CANCELLED) {
                log.info("worker skips cancelled run={} key={} version={}", run.runId(),
                        run.idempotencyKey(), run.definitionVersion());
                return;
            }
            DefinitionVersion version =
                    definitionService.getVersion(pipelineId, run.definitionVersion());
            if (version.status() == DefinitionStatus.WITHDRAWN) {
                fail(run, RunStatus.FAILED_FATAL, FailureReason.DEFINITION_WITHDRAWN,
                        "definition version " + run.definitionVersion() + " was withdrawn before execution");
                return;
            }
            transition(run, RunStatus.RUNNING, EventType.RUN_STARTED,
                    "worker starts in " + run.currentEnvironment());
        }

        try {
            runEnvironment(run, lock);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            synchronized (lock) {
                if (run.status() != RunStatus.CANCELLED) {
                    fail(run, RunStatus.FAILED_FATAL, FailureReason.STAGE_FAILED_FATAL,
                            "worker interrupted: " + e.getMessage());
                }
            }
        } catch (RuntimeException e) {
            synchronized (lock) {
                if (!isTerminal(run.status())) {
                    fail(run, RunStatus.FAILED_FATAL, FailureReason.STAGE_FAILED_FATAL,
                            "unexpected error: " + e);
                }
            }
        }
    }

    private void runEnvironment(RunRecord scheduled, Object lock) throws InterruptedException {
        while (true) {
            // --- phase 1: under the lock, decide what (if anything) to do next ---
            RunRecord run;
            StageSpec stage;
            int attemptSnapshot;
            synchronized (lock) {
                run = requireRun(scheduled.runId());
                if (run.status() != RunStatus.RUNNING) {
                    // CANCELLED, or externally transitioned (manual retry/promotion): stop this worker.
                    return;
                }
                PipelineDefinition definition = definitionService.getVersion(run.pipelineId(),
                        run.definitionVersion()).definition();
                stage = nextApplicableStage(definition, run);
                if (stage == null) {
                    finishEnvironment(run, definition);
                    return;
                }
                attemptSnapshot = run.attempt();
                auditLog.append(EventType.STAGE_STARTED, run.pipelineId(), run.runId(),
                        run.idempotencyKey(), run.definitionVersion(), run.status(), run.status(),
                        "stage '" + stage.name() + "' attempt " + attemptSnapshot + " in "
                                + run.currentEnvironment());
                log.info("stage start run={} key={} version={} stage={} env={} attempt={}",
                        run.runId(), run.idempotencyKey(), run.definitionVersion(), stage.name(),
                        run.currentEnvironment(), attemptSnapshot);
            }

            // --- phase 2: execute WITHOUT the lock so cancel/promote can land concurrently ---
            StageExecutor.StageResult result = stageExecutor.execute(run.runId(), run.pipelineId(),
                    run.definitionVersion(), stage, run.currentEnvironment(), attemptSnapshot);

            // --- phase 3: re-acquire the lock, validate, then apply the outcome atomically ---
            long backoff;
            synchronized (lock) {
                run = requireRun(scheduled.runId());
                if (run.status() != RunStatus.RUNNING) {
                    log.info("stage result discarded run={} key={} version={} status={}",
                            run.runId(), run.idempotencyKey(), run.definitionVersion(), run.status());
                    return;
                }
                // The run may have been re-pointed (retry from another stage index/env): only apply
                // the result if the cursor still matches the stage we just executed.
                PipelineDefinition definition = definitionService.getVersion(run.pipelineId(),
                        run.definitionVersion()).definition();
                StageSpec currentStage = nextApplicableStage(definition, run);
                if (currentStage == null || !currentStage.name().equals(stage.name())
                        || !run.currentEnvironment().equals(scheduled.currentEnvironment())
                        || run.attempt() != attemptSnapshot) {
                    continue;
                }
                run.stageExecutions().add(new StageExecution(stage.name(), run.currentEnvironment(),
                        attemptSnapshot, result.success(), result.fatal(), result.message(),
                        Instant.now()));

                if (result.success()) {
                    run.attempt(1);
                    run.currentStageIndex(run.currentStageIndex() + 1);
                    runRepository.save(run);
                    auditLog.append(EventType.STAGE_SUCCEEDED, run.pipelineId(), run.runId(),
                            run.idempotencyKey(), run.definitionVersion(), run.status(), run.status(),
                            "stage '" + stage.name() + "' succeeded in " + run.currentEnvironment());
                    continue;
                }

                auditLog.append(EventType.STAGE_FAILED, run.pipelineId(), run.runId(),
                        run.idempotencyKey(), run.definitionVersion(), run.status(), run.status(),
                        "stage '" + stage.name() + "' failed (fatal=" + result.fatal()
                                + ", retryableStage=" + stage.retryable() + "): " + result.message());
                boolean nonRetryable = result.fatal() || !stage.retryable();
                if (nonRetryable) {
                    fail(run, RunStatus.FAILED_FATAL, FailureReason.STAGE_FAILED_FATAL,
                            "stage '" + stage.name() + "' failed with non-retryable error: "
                                    + result.message());
                    return;
                }
                if (run.attempt() < retryPolicy.maxAttempts()) {
                    int nextAttempt = run.attempt() + 1;
                    run.status(RunStatus.FAILED_RETRYABLE);
                    run.failureReason(FailureReason.STAGE_FAILED_RETRYABLE);
                    run.failureMessage(result.message());
                    run.attempt(nextAttempt);
                    runRepository.save(run);
                    backoff = retryBackoff.delayMillis(retryPolicy, nextAttempt - 1);
                    pendingAutoRetry.add(run.runId());
                } else {
                    fail(run, RunStatus.FAILED_RETRYABLE, FailureReason.RETRIES_EXHAUSTED,
                            "stage '" + stage.name() + "' exhausted " + retryPolicy.maxAttempts()
                                    + " attempts; last error: " + result.message());
                    return;
                }
            }

            // sleep outside the pipeline lock so cancellation can land during backoff
            if (backoff > 0) {
                log.info("backoff run={} key={} version={} millis={} attempt={}", scheduled.runId(),
                        scheduled.idempotencyKey(), run.definitionVersion(), backoff,
                        scheduled.attempt());
                Thread.sleep(backoff);
            }
            synchronized (lock) {
                pendingAutoRetry.remove(scheduled.runId());
                RunRecord woke = requireRun(scheduled.runId());
                if (woke.status() == RunStatus.CANCELLED) {
                    log.info("auto-retry aborted after cancellation run={} key={} version={}",
                            woke.runId(), woke.idempotencyKey(), woke.definitionVersion());
                    return;
                }
                transition(woke, RunStatus.RUNNING, EventType.RUN_RETRY,
                        "automatic retry attempt " + woke.attempt());
            }
        }
    }

    private StageSpec nextApplicableStage(PipelineDefinition definition, RunRecord run) {
        List<StageSpec> stages = definition.stages();
        for (int i = run.currentStageIndex(); i < stages.size(); i++) {
            if (stages.get(i).environments().contains(run.currentEnvironment())) {
                return stages.get(i);
            }
            run.currentStageIndex(i + 1);
        }
        return null;
    }

    private void finishEnvironment(RunRecord run, PipelineDefinition definition) {
        List<String> envs = definition.environments();
        int envIndex = envs.indexOf(run.currentEnvironment());
        if (envIndex + 1 < envs.size()) {
            transition(run, RunStatus.WAITING_PROMOTION, EventType.RUN_STARTED,
                    "environment " + run.currentEnvironment()
                            + " completed; waiting for promotion to " + envs.get(envIndex + 1));
            return;
        }
        // Publish first (while the run is still RUNNING); only mark SUCCEEDED afterwards so an
        // outside observer never sees SUCCEEDED without the corresponding release + audit event.
        releaseService.publish(run.pipelineId(), run.currentEnvironment(),
                run.definitionVersion(), run.runId());
        run.failureReason(FailureReason.NONE);
        run.failureMessage(null);
        RunStatus from = run.status();
        run.status(RunStatus.SUCCEEDED);
        runRepository.save(run);
        auditLog.append(EventType.RUN_SUCCEEDED, run.pipelineId(), run.runId(), run.idempotencyKey(),
                run.definitionVersion(), from, RunStatus.SUCCEEDED,
                "all environments completed and release published to " + run.currentEnvironment());
        log.info("succeeded run={} key={} version={} {} -> SUCCEEDED env={}", run.runId(),
                run.idempotencyKey(), run.definitionVersion(), from, run.currentEnvironment());
    }

    private void fail(RunRecord run, RunStatus status, FailureReason reason, String message) {
        RunStatus from = run.status();
        run.status(status);
        run.failureReason(reason);
        run.failureMessage(message);
        runRepository.save(run);
        auditLog.append(EventType.RUN_FAILED, run.pipelineId(), run.runId(), run.idempotencyKey(),
                run.definitionVersion(), from, status, reason + ": " + message);
        log.info("failed run={} key={} version={} {} -> {} reason={} message={}", run.runId(),
                run.idempotencyKey(), run.definitionVersion(), from, status, reason, message);
    }

    private void transition(RunRecord run, RunStatus to, EventType event, String detail) {
        RunStatus from = run.status();
        run.status(to);
        runRepository.save(run);
        auditLog.append(event, run.pipelineId(), run.runId(), run.idempotencyKey(),
                run.definitionVersion(), from, to, detail);
        log.info("transition run={} key={} version={} {} -> {} ({})", run.runId(),
                run.idempotencyKey(), run.definitionVersion(), from, to, detail);
    }

    // --------------------------------------------------------------- capacity

    private int inFlight() {
        return pool.getActiveCount() + pool.getQueue().size();
    }

    private int capacityLimit() {
        int threads = Math.max(1, properties.getMaxConcurrentRuns());
        return properties.getRejectPolicy() == RejectPolicy.REJECT
                ? threads
                : threads + Math.max(1, properties.getQueueCapacity());
    }

    private boolean hasCapacity() {
        return inFlight() < capacityLimit();
    }

    private void schedule(RunRecord run) {
        try {
            pool.execute(() -> executeQueuedRun(run));
        } catch (RejectedExecutionException e) {
            auditLog.append(EventType.SUBMISSION_REJECTED, run.pipelineId(), run.runId(),
                    run.idempotencyKey(), run.definitionVersion(), run.status(), run.status(),
                    "rejected by bounded worker pool");
            throw new CapacityRejectedException("worker pool saturated for pipeline "
                    + run.pipelineId());
        }
    }

    private RunRecord requireRun(String runId) {
        return runRepository.findById(runId)
                .orElseThrow(() -> new RunNotFoundException("run not found: " + runId));
    }

    private boolean isTerminal(RunStatus s) {
        return s == RunStatus.SUCCEEDED || s == RunStatus.FAILED_FATAL || s == RunStatus.CANCELLED;
    }

    /** Stop worker threads and release bounded resources. Called on application shutdown / test teardown. */
    @jakarta.annotation.PreDestroy
    public void shutdown() {
        pool.shutdownNow();
        try {
            if (!pool.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("pipeline worker pool did not terminate cleanly");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
