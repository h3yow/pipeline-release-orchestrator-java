package com.github.h3yow.pipelinereleaseorchestratorjava.tests;

import com.github.h3yow.pipelinereleaseorchestratorjava.exception.DefinitionRejectedException;
import com.github.h3yow.pipelinereleaseorchestratorjava.exception.IllegalTransitionException;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.AuditType;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.DefinitionVersion;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.PipelineRun;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.RejectReason;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.ReleaseRecord;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.RunStatus;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.StageSpec;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.StageStatus;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.stage.StageHandler;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.stage.StageResult;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.h3yow.pipelinereleaseorchestratorjava.tests.TestEnv.definition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Mid-run invalidation when a bound definition version is withdrawn. */
class WithdrawInvalidationTests {

    private static final Logger log = LoggerFactory.getLogger(WithdrawInvalidationTests.class);

    @Test
    void withdrawInvalidatesQueuedRunsAndReleasesQueueQuota() {
        TestEnv env = TestEnv.create((stage, run, attempt) -> StageResult.ok(), "QUEUE");
        // maxInFlight=2, maxQueued=2 (TestEnv defaults)
        DefinitionVersion v1 = env.definitions.createVersion("svc",
                definition("svc", 0, Duration.ZERO, List.of("dev"), "build"));
        PipelineRun r1 = env.runs.submit("svc", v1.getVersion(), "q-1", Map.of());
        PipelineRun r2 = env.runs.submit("svc", v1.getVersion(), "q-2", Map.of());
        PipelineRun r3 = env.runs.submit("svc", v1.getVersion(), "q-3", Map.of());
        PipelineRun r4 = env.runs.submit("svc", v1.getVersion(), "q-4", Map.of());
        assertTrue(r3.isQueued() && r4.isQueued(), "r3/r4 should be buffered in the queue");

        env.definitions.withdraw("svc", v1.getVersion(), "broken spec");

        for (PipelineRun r : List.of(r1, r2, r3, r4)) {
            PipelineRun after = env.runs.get(r.getRunId());
            log.info("[WITHDRAW] key={} status={} code={}", r.getIdempotencyKey(),
                    after.getStatus(), after.getFailureCode());
            assertEquals(RunStatus.INVALIDATED, after.getStatus());
            assertEquals(RejectReason.DEFINITION_WITHDRAWN, after.getFailureCode());
            assertFalse(after.isQueued(), "invalidated run must release its queue slot");
            assertTrue(after.getStatus().isTerminal());
        }

        // Queue quota was released: a new version accepts a full batch again.
        DefinitionVersion v2 = env.definitions.createVersion("svc",
                definition("svc", 0, Duration.ZERO, List.of("dev"), "build"));
        for (int i = 1; i <= 4; i++) {
            PipelineRun r = env.runs.submit("svc", v2.getVersion(), "q2-" + i, Map.of());
            assertEquals(RunStatus.PENDING, r.getStatus());
        }
        // ... and the cap still applies on top of the released quota.
        assertThrows(com.github.h3yow.pipelinereleaseorchestratorjava.exception.OverflowException.class,
                () -> env.runs.submit("svc", v2.getVersion(), "q2-5", Map.of()));
    }

    @Test
    void withdrawInvalidatesWaitingRetryRunAndBlocksFurtherRetry() {
        StageHandler flaky = new StageHandler() {
            private final AtomicInteger calls = new AtomicInteger();

            @Override
            public StageResult execute(StageSpec stage, PipelineRun run, int attempt) {
                calls.incrementAndGet();
                return StageResult.fail(true, "transient");
            }
        };
        TestEnv env = TestEnv.create(flaky);
        DefinitionVersion v = env.definitions.createVersion("svc",
                definition("svc", 5, Duration.ofMinutes(10), List.of("dev"), "build", "test"));
        PipelineRun run = env.runs.submit("svc", v.getVersion(), "w-retry", Map.of());
        env.runs.advance(run.getRunId());
        assertEquals(RunStatus.WAITING_RETRY, env.runs.get(run.getRunId()).getStatus());

        env.definitions.withdraw("svc", v.getVersion(), "spec reverted");

        PipelineRun after = env.runs.get(run.getRunId());
        log.info("[WITHDRAW] key=w-retry status={} code={} reason={}",
                after.getStatus(), after.getFailureCode(), after.getFailureReason());
        // Distinct from ordinary failure and ordinary cancellation.
        assertEquals(RunStatus.INVALIDATED, after.getStatus());
        assertNotEquals(RunStatus.FAILED, after.getStatus());
        assertNotEquals(RunStatus.CANCELLED, after.getStatus());
        assertEquals(RejectReason.DEFINITION_WITHDRAWN, after.getFailureCode());
        assertTrue(after.getFailureReason().contains("spec reverted"));
        // The previously failed stage record is preserved as evidence.
        assertEquals(StageStatus.FAILED, after.getStages().get(0).getStatus());
        assertEquals("transient", after.getStages().get(0).getLastError());
        assertEquals(StageStatus.PENDING, after.getStages().get(1).getStatus());

        // The withdrawn version must never execute again: retry and advance are rejected.
        IllegalTransitionException retryEx = assertThrows(IllegalTransitionException.class,
                () -> env.runs.retry(run.getRunId()));
        assertEquals(RejectReason.DEFINITION_WITHDRAWN, retryEx.getReason());
        assertThrows(IllegalTransitionException.class, () -> env.runs.advance(run.getRunId()));
    }

    @Test
    void withdrawDuringExecutionStopsBeforeNextStageAndKeepsCompletedStages() throws Exception {
        CountDownLatch buildEntered = new CountDownLatch(1);
        CountDownLatch releaseBuild = new CountDownLatch(1);
        AtomicInteger stageCalls = new AtomicInteger();
        StageHandler gated = (StageSpec stage, PipelineRun run, int attempt) -> {
            stageCalls.incrementAndGet();
            if ("build".equals(stage.name())) {
                buildEntered.countDown();
                try {
                    releaseBuild.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return StageResult.ok();
        };
        TestEnv env = TestEnv.create(gated);
        DefinitionVersion v = env.definitions.createVersion("svc",
                definition("svc", 0, Duration.ZERO, List.of("dev"), "build", "test", "deploy"));
        PipelineRun run = env.runs.submit("svc", v.getVersion(), "w-running", Map.of());

        ExecutorService pool = Executors.newFixedThreadPool(2);
        Future<?> advance = pool.submit(() -> env.runs.advance(run.getRunId()));
        assertTrue(buildEntered.await(5, TimeUnit.SECONDS), "build should be executing");

        // Withdraw while the stage runs: the status flip is lock-free, so the drive
        // loop observes it as soon as the in-flight stage completes.
        Future<?> withdraw = pool.submit(
                () -> env.definitions.withdraw("svc", v.getVersion(), "urgent rollback of spec"));
        // Give the withdraw call a moment to flip the volatile status, then let the
        // in-flight stage finish honestly.
        Thread.sleep(100);
        releaseBuild.countDown();

        advance.get(5, TimeUnit.SECONDS);
        withdraw.get(5, TimeUnit.SECONDS);
        pool.shutdown();

        PipelineRun after = env.runs.get(run.getRunId());
        log.info("[WITHDRAW] key=w-running status={} stages={}", after.getStatus(),
                after.getStages().stream().map(s -> s.getName() + "=" + s.getStatus()).toList());
        assertEquals(RunStatus.INVALIDATED, after.getStatus());
        // The stage that genuinely completed keeps its success record...
        assertEquals(StageStatus.SUCCEEDED, after.getStages().get(0).getStatus());
        // ...but no later stage of the withdrawn version ever executed.
        assertEquals(StageStatus.PENDING, after.getStages().get(1).getStatus());
        assertEquals(StageStatus.PENDING, after.getStages().get(2).getStatus());
        assertEquals(0, after.getStages().get(1).getAttempts());
        assertEquals(1, stageCalls.get(), "only the in-flight stage ran");
        assertEquals(RejectReason.DEFINITION_WITHDRAWN, after.getFailureCode());
        // Audit trail shows the withdrawal and the invalidation in order.
        List<AuditType> types = env.store.auditTrail("svc").stream()
                .map(e -> e.getType()).toList();
        assertTrue(types.contains(AuditType.DEFINITION_WITHDRAWN));
        assertTrue(types.contains(AuditType.RUN_INVALIDATED));
    }

    @Test
    void withdrawLeavesOtherVersionsRunsAndReleaseHistoryUntouched() {
        TestEnv env = TestEnv.create((stage, run, attempt) -> StageResult.ok());
        DefinitionVersion v1 = env.definitions.createVersion("svc",
                definition("svc", 0, Duration.ZERO, List.of("dev"), "build"));
        PipelineRun succeeded = env.runs.submit("svc", v1.getVersion(), "keep-1", Map.of());
        env.runs.advance(succeeded.getRunId());
        assertEquals(RunStatus.SUCCEEDED, env.runs.get(succeeded.getRunId()).getStatus());
        ReleaseRecord release = env.releases.promote(succeeded.getRunId());

        DefinitionVersion v2 = env.definitions.createVersion("svc",
                definition("svc", 0, Duration.ZERO, List.of("dev"), "build"));
        PipelineRun onV2 = env.runs.submit("svc", v2.getVersion(), "keep-2", Map.of());
        PipelineRun onV1 = env.runs.submit("svc", v1.getVersion(), "doomed", Map.of());

        env.definitions.withdraw("svc", v1.getVersion(), "v1 revoked");

        // The v1 run accepted but not finished is invalidated...
        assertEquals(RunStatus.INVALIDATED, env.runs.get(onV1.getRunId()).getStatus());
        // ...the already finished v1 run and its release history are untouched...
        assertEquals(RunStatus.SUCCEEDED, env.runs.get(succeeded.getRunId()).getStatus());
        List<ReleaseRecord> history = env.releases.history("svc");
        assertEquals(1, history.size());
        assertEquals(release.getReleaseId(), history.get(0).getReleaseId());
        // ...and the v2 run is completely unaffected: it still executes and promotes.
        assertEquals(RunStatus.PENDING, env.runs.get(onV2.getRunId()).getStatus());
        env.runs.advance(onV2.getRunId());
        assertEquals(RunStatus.SUCCEEDED, env.runs.get(onV2.getRunId()).getStatus());
        env.releases.promote(onV2.getRunId());
        assertEquals(2, env.releases.history("svc").size());

        // New submissions bound to the withdrawn version are rejected distinctly.
        DefinitionRejectedException ex = assertThrows(DefinitionRejectedException.class,
                () -> env.runs.submit("svc", v1.getVersion(), "late-submit", Map.of()));
        assertEquals(RejectReason.DEFINITION_WITHDRAWN, ex.getReason());
        // An invalidated run can never be promoted.
        assertThrows(IllegalTransitionException.class,
                () -> env.releases.promote(onV1.getRunId()));
    }

    @Test
    void idempotencyKeyStaysStableAcrossInvalidation() {
        TestEnv env = TestEnv.create((stage, run, attempt) -> StageResult.ok());
        DefinitionVersion v = env.definitions.createVersion("svc",
                definition("svc", 0, Duration.ZERO, List.of("dev"), "build"));
        PipelineRun run = env.runs.submit("svc", v.getVersion(), "idem-w", Map.of("a", 1));

        // Before the interruption: duplicate submit dedups to the same run.
        PipelineRun dupBefore = env.runs.submit("svc", v.getVersion(), "idem-w", Map.of("a", 1));
        assertEquals(run.getRunId(), dupBefore.getRunId());

        env.definitions.withdraw("svc", v.getVersion(), "revoked");
        assertEquals(RunStatus.INVALIDATED, env.runs.get(run.getRunId()).getStatus());

        // After the interruption the key still maps to exactly that one run: the
        // withdrawn version rejects any new submission distinctly (no new run is
        // ever created for the key) and the original run keeps its final result.
        DefinitionRejectedException ex = assertThrows(DefinitionRejectedException.class,
                () -> env.runs.submit("svc", v.getVersion(), "idem-w", Map.of("a", 1)));
        assertEquals(RejectReason.DEFINITION_WITHDRAWN, ex.getReason());
        PipelineRun after = env.runs.get(run.getRunId());
        log.info("[WITHDRAW] key=idem-w runId={} status={} runsTotal={}",
                after.getRunId(), after.getStatus(), env.store.listRuns("svc").size());
        assertEquals(RunStatus.INVALIDATED, after.getStatus());
        assertEquals(1, env.store.listRuns("svc").size(),
                "duplicate submissions must never create another run");
    }
}
