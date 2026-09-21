package com.github.h3yow.pipelinereleaseorchestratorjava.tests;

import com.github.h3yow.pipelinereleaseorchestratorjava.config.OrchestratorProperties;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.DefinitionVersion;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.PipelineDefinition;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.RejectPolicy;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.RunRecord;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.RunStatus;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.StageSpec;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.DefinitionService;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.PipelineOrchestrator;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.ReleaseService;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.RetryBackoff;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.StageExecutor;
import com.github.h3yow.pipelinereleaseorchestratorjava.store.AuditLog;
import com.github.h3yow.pipelinereleaseorchestratorjava.store.DefinitionRepository;
import com.github.h3yow.pipelinereleaseorchestratorjava.store.JsonFileStore;
import com.github.h3yow.pipelinereleaseorchestratorjava.store.ReleaseRepository;
import com.github.h3yow.pipelinereleaseorchestratorjava.store.RunRepository;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntPredicate;

/** Wires the whole service graph against a temp directory with a scripted stage executor. */
final class TestHarness implements AutoCloseable {

    private final JsonFileStore store;
    final AuditLog auditLog;
    final DefinitionService definitionService;
    final ReleaseService releaseService;
    final PipelineOrchestrator orchestrator;
    final ScriptedStageExecutor stageExecutor;

    TestHarness(Path tempDir) {
        this(tempDir, defaultProperties());
    }

    TestHarness(Path tempDir, OrchestratorProperties properties) {
        this.store = new JsonFileStore(tempDir.toString());
        DefinitionRepository definitionRepository = new DefinitionRepository(store);
        RunRepository runRepository = new RunRepository(store);
        ReleaseRepository releaseRepository = new ReleaseRepository(store);
        this.auditLog = new AuditLog(store);
        this.stageExecutor = new ScriptedStageExecutor();
        this.definitionService = new DefinitionService(definitionRepository, auditLog);
        this.releaseService = new ReleaseService(releaseRepository, definitionRepository, auditLog);
        this.orchestrator = new PipelineOrchestrator(definitionService, runRepository, releaseService,
                auditLog, stageExecutor, new RetryBackoff(), properties);
        HarnessCleanup.register(this);
    }


    @Override
    public void close() {
        orchestrator.shutdown();
    }

    static OrchestratorProperties defaultProperties() {
        OrchestratorProperties p = new OrchestratorProperties();
        p.setMaxConcurrentRuns(4);
        p.setQueueCapacity(20);
        p.setRejectPolicy(RejectPolicy.QUEUE);
        p.setDefaultMaxAttempts(3);
        p.setDefaultInitialBackoffMillis(10);
        p.setDefaultMaxBackoffMillis(100);
        return p;
    }

    DefinitionVersion define(String pipelineId, List<String> envs, List<StageSpec> stages) {
        return definitionService.createOrGet(pipelineId,
                new PipelineIdName(pipelineId, envs, stages).definition());
    }

    RunRecord awaitTerminal(String runId) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        RunRecord run = orchestrator.getRun(runId);
        while (!isTerminal(run.status()) && System.nanoTime() < deadline) {
            Thread.sleep(5);
            run = orchestrator.getRun(runId);
        }
        if (!isTerminal(run.status())) {
            throw new AssertionError("run " + runId + " did not terminate, status=" + run.status());
        }
        return run;
    }

    RunRecord awaitStatus(String runId, RunStatus status) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        RunRecord run = orchestrator.getRun(runId);
        while (run.status() != status && System.nanoTime() < deadline) {
            Thread.sleep(5);
            run = orchestrator.getRun(runId);
        }
        if (run.status() != status) {
            throw new AssertionError(
                    "run " + runId + " reached " + run.status() + ", expected " + status);
        }
        return run;
    }

    RunRecord awaitFailureReason(String runId,
                                 com.github.h3yow.pipelinereleaseorchestratorjava.model.FailureReason reason)
            throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        RunRecord run = orchestrator.getRun(runId);
        boolean terminalSettled = isTerminal(run.status())
                || (run.status() == RunStatus.FAILED_RETRYABLE
                        && run.failureReason() == reason);
        while (!terminalSettled && System.nanoTime() < deadline) {
            Thread.sleep(5);
            run = orchestrator.getRun(runId);
            terminalSettled = isTerminal(run.status())
                    || (run.status() == RunStatus.FAILED_RETRYABLE
                            && run.failureReason() == reason);
        }
        if (run.failureReason() != reason) {
            throw new AssertionError("run " + runId + " failureReason=" + run.failureReason()
                    + ", expected " + reason + " (status=" + run.status() + ")");
        }
        return run;
    }

    static boolean isTerminal(RunStatus s) {
        return s == RunStatus.SUCCEEDED || s == RunStatus.FAILED_FATAL
                || s == RunStatus.CANCELLED;
    }

    private record PipelineIdName(String pipelineId, List<String> envs, List<StageSpec> stages) {
        PipelineDefinition definition() {
            return new PipelineDefinition("pipe-" + pipelineId, envs, stages);
        }
    }

    /** Executor whose outcome per (stage, environment) is scripted; tracks execution counts. */
    static final class ScriptedStageExecutor implements StageExecutor {
        private final ConcurrentMap<String, IntPredicate> scripts = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, AtomicInteger> counts = new ConcurrentHashMap<>();
        private final java.util.Set<String> fatalKeys =
                java.util.Collections.newSetFromMap(new ConcurrentHashMap<>());
        private volatile long stageDelayMillis = 0L;

        /** Fail attempts for which predicate returns true; all later attempts succeed. */
        void failAttempts(String stage, String env, IntPredicate failingAttempts) {
            scripts.put(stage + "|" + env, failingAttempts);
        }

        void alwaysFail(String stage, String env) {
            scripts.put(stage + "|" + env, attempt -> true);
        }

        /** Fail the given attempts with a fatal (non-retryable regardless of stage config) result. */
        void fatalAttempts(String stage, String env, IntPredicate failingAttempts) {
            scripts.put(stage + "|" + env, failingAttempts);
            fatalKeys.add(stage + "|" + env);
        }

        void setStageDelayMillis(long millis) {
            this.stageDelayMillis = millis;
        }

        int executionCount(String stage, String env) {
            AtomicInteger counter = counts.get(stage + "|" + env);
            return counter == null ? 0 : counter.get();
        }

        @Override
        public StageResult execute(String runId, String pipelineId, String versionId, StageSpec stage,
                                   String environment, int attempt) {
            if (stageDelayMillis > 0) {
                try {
                    Thread.sleep(stageDelayMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            String key = stage.name() + "|" + environment;
            counts.computeIfAbsent(key, k -> new AtomicInteger()).incrementAndGet();
            IntPredicate predicate = scripts.get(key);
            if (predicate != null && predicate.test(attempt)) {
                if (fatalKeys.contains(key)) {
                    return StageResult.fatalFailure("scripted fatal failure on attempt " + attempt);
                }
                return StageResult.retryableFailure("scripted failure on attempt " + attempt);
            }
            return StageResult.ok();
        }
    }
}
