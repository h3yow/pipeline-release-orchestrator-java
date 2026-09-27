package com.github.h3yow.pipelinereleaseorchestratorjava.tests;

import com.github.h3yow.pipelinereleaseorchestratorjava.exception.DefinitionRejectedException;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.AuditEvent;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.AuditType;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.DefinitionVersion;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.PipelineRun;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.RejectReason;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.ReleaseRecord;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.ReleaseStatus;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.RunStatus;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.StageSpec;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.StageStatus;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.stage.AlwaysSucceedStageHandler;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefinitionRevocationTests {

    private static final Logger log = LoggerFactory.getLogger(DefinitionRevocationTests.class);

    @Test
    void withdrawTerminatesQueuedAndPendingRunsAndReleasesCapacity() {
        TestEnv env = TestEnv.create(new AlwaysSucceedStageHandler(), "QUEUE");
        DefinitionVersion v1 = env.definitions.createVersion("svc",
                definition("svc", 0, Duration.ZERO, List.of("dev"), "build"));
        // maxInFlight=2, maxQueued=2: two occupying slots + two queued, all PENDING.
        PipelineRun r1 = env.runs.submit("svc", v1.getVersion(), "q-1", Map.of());
        PipelineRun r2 = env.runs.submit("svc", v1.getVersion(), "q-2", Map.of());
        PipelineRun r3 = env.runs.submit("svc", v1.getVersion(), "q-3", Map.of());
        PipelineRun r4 = env.runs.submit("svc", v1.getVersion(), "q-4", Map.of());
        assertTrue(r3.isQueued() && r4.isQueued());

        env.definitions.withdraw("svc", v1.getVersion(), "broken definition");

        for (PipelineRun r : List.of(r1, r2, r3, r4)) {
            PipelineRun after = env.runs.get(r.getRunId());
            assertEquals(RunStatus.DEFINITION_REVOKED, after.getStatus());
            assertEquals(RejectReason.DEFINITION_WITHDRAWN, after.getFailureCode());
            assertEquals("broken definition", after.getFailureReason());
            // Distinct from a normal failure and a normal cancel.
            assertTrue(after.getStatus() != RunStatus.FAILED
                    && after.getStatus() != RunStatus.FAILED_NON_RETRYABLE
                    && after.getStatus() != RunStatus.CANCELLED);
        }
        log.info("[REVOKE] queued+pending runs terminated; inFlight={}",
                env.store.countInFlight("svc"));
        assertEquals(0, env.store.countInFlight("svc"), "capacity and queue slots released");

        // A new version on the same pipeline is accepted under the same limits.
        DefinitionVersion v2 = env.definitions.createVersion("svc",
                definition("svc", 0, Duration.ZERO, List.of("dev"), "build"));
        PipelineRun n1 = env.runs.submit("svc", v2.getVersion(), "n-1", Map.of());
        PipelineRun n2 = env.runs.submit("svc", v2.getVersion(), "n-2", Map.of());
        PipelineRun n3 = env.runs.submit("svc", v2.getVersion(), "n-3", Map.of());
        assertTrue(n3.isQueued(), "queue works again after revocation released slots");
        log.info("[REVOKE] new version accepted: {} {} (queued={})",
                n1.getRunId(), n2.getRunId(), n3.isQueued());
    }

    @Test
    void withdrawTerminatesWaitingRetryRunAndKeepsStageHistory() {
        AtomicInteger calls = new AtomicInteger();
        StageHandler flaky = (stage, run, attempt) -> {
            calls.incrementAndGet();
            return StageResult.fail(true, "transient");
        };
        TestEnv env = TestEnv.create(flaky);
        DefinitionVersion v = env.definitions.createVersion("svc",
                definition("svc", 3, Duration.ofMinutes(5), List.of("dev"), "build", "test"));
        PipelineRun run = env.runs.submit("svc", v.getVersion(), "wr-1", Map.of());
        env.runs.advance(run.getRunId());
        assertEquals(RunStatus.WAITING_RETRY, env.runs.get(run.getRunId()).getStatus());

        env.definitions.withdraw("svc", v.getVersion(), "bad backoff config");

        PipelineRun after = env.runs.get(run.getRunId());
        log.info("[REVOKE] waiting-retry run status={} code={} stages={}",
                after.getStatus(), after.getFailureCode(),
                after.getStages().stream().map(s -> s.getName() + "=" + s.getStatus()).toList());
        assertEquals(RunStatus.DEFINITION_REVOKED, after.getStatus());
        assertEquals(RejectReason.DEFINITION_WITHDRAWN, after.getFailureCode());
        // The earlier attempt is preserved as history, not erased.
        assertEquals(StageStatus.FAILED, after.getStages().get(0).getStatus());
        assertEquals(1, after.getStages().get(0).getAttempts());
        assertEquals(StageStatus.PENDING, after.getStages().get(1).getStatus());
        // No further execution of the withdrawn version is possible.
        assertThrows(Exception.class, () -> env.runs.advance(run.getRunId()));
        assertThrows(Exception.class, () -> env.runs.retry(run.getRunId()));
        assertEquals(1, calls.get(), "withdrawn version never executes again");
    }

    @Test
    void withdrawInterruptsRunningRunMidExecution() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        AtomicInteger stageCalls = new AtomicInteger();
        StageHandler blocking = new StageHandler() {
            @Override
            public StageResult execute(StageSpec stage, PipelineRun run, int attempt)
                    throws InterruptedException {
                stageCalls.incrementAndGet();
                if ("build".equals(stage.name())) {
                    entered.countDown();
                    Thread.sleep(10_000); // interrupted by the withdrawal
                }
                return StageResult.ok();
            }
        };
        TestEnv env = TestEnv.create(blocking);
        DefinitionVersion v = env.definitions.createVersion("svc",
                definition("svc", 0, Duration.ZERO, List.of("dev"), "build", "test"));
        PipelineRun run = env.runs.submit("svc", v.getVersion(), "run-mid", Map.of());

        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<?> advance = pool.submit(() -> env.runs.advance(run.getRunId()));
        assertTrue(entered.await(5, TimeUnit.SECONDS));

        env.definitions.withdraw("svc", v.getVersion(), "urgent recall");
        advance.get(5, TimeUnit.SECONDS);
        pool.shutdown();

        PipelineRun after = env.runs.get(run.getRunId());
        log.info("[REVOKE] running run status={} code={} stages={}",
                after.getStatus(), after.getFailureCode(),
                after.getStages().stream().map(s -> s.getName() + "=" + s.getStatus()).toList());
        assertEquals(RunStatus.DEFINITION_REVOKED, after.getStatus());
        assertEquals(RejectReason.DEFINITION_WITHDRAWN, after.getFailureCode());
        assertEquals(StageStatus.INTERRUPTED, after.getStages().get(0).getStatus());
        assertEquals(StageStatus.PENDING, after.getStages().get(1).getStatus());
        assertEquals(1, stageCalls.get(), "the next stage of the withdrawn version never ran");

        List<AuditType> types = env.store.auditTrail("svc").stream()
                .map(AuditEvent::getType).toList();
        assertTrue(types.contains(AuditType.DEFINITION_WITHDRAWN));
        assertTrue(types.contains(AuditType.RUN_DEFINITION_REVOKED));
    }

    @Test
    void withdrawLandingAfterStageTrulyCompletedKeepsStageSuccess() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        AtomicInteger stageCalls = new AtomicInteger();
        StageHandler completesDespiteInterrupt = new StageHandler() {
            @Override
            public StageResult execute(StageSpec stage, PipelineRun run, int attempt) {
                stageCalls.incrementAndGet();
                if ("build".equals(stage.name())) {
                    entered.countDown();
                    // The withdrawal interrupt IS the deterministic signal that the
                    // revocation intent was registered while this stage was executing.
                    // The stage does not respond to it (uninterruptible work) and truly
                    // runs to completion, returning success inside the revocation window.
                    awaitInterrupt();
                }
                return StageResult.ok();
            }
        };
        TestEnv env = TestEnv.create(completesDespiteInterrupt);
        DefinitionVersion v = env.definitions.createVersion("svc",
                definition("svc", 0, Duration.ZERO, List.of("dev"), "build", "test", "deploy"));
        PipelineRun run = env.runs.submit("svc", v.getVersion(), "revoke-window", Map.of());

        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<?> advance = pool.submit(() -> env.runs.advance(run.getRunId()));
        assertTrue(entered.await(5, TimeUnit.SECONDS), "stage should have started");

        // Called on this thread: the lock-free signal interrupts the executing stage (which
        // then truly completes); the lock-held part proceeds once the drive loop converges.
        env.definitions.withdraw("svc", v.getVersion(), "urgent recall");
        advance.get(5, TimeUnit.SECONDS);
        pool.shutdown();

        PipelineRun after = env.runs.get(run.getRunId());
        log.info("[REVOKE] key=revoke-window status={} code={} stages={}",
                after.getStatus(), after.getFailureCode(),
                after.getStages().stream().map(s -> s.getName() + "=" + s.getStatus()).toList());

        // The run converges to DEFINITION_REVOKED with its distinct reason...
        assertEquals(RunStatus.DEFINITION_REVOKED, after.getStatus());
        assertEquals(RejectReason.DEFINITION_WITHDRAWN, after.getFailureCode());
        assertEquals("urgent recall", after.getFailureReason());
        // ...but the stage that truly ran to completion keeps its success as-is...
        assertEquals(StageStatus.SUCCEEDED, after.getStages().get(0).getStatus());
        assertEquals(null, after.getStages().get(0).getLastError());
        // ...and the stages of the withdrawn version that never ran keep no success trace.
        assertEquals(StageStatus.PENDING, after.getStages().get(1).getStatus());
        assertEquals(StageStatus.PENDING, after.getStages().get(2).getStatus());
        assertEquals(1, stageCalls.get(), "the next stage of the withdrawn version never ran");
        assertEquals(0, env.store.countInFlight("svc"), "capacity released");

        // The audit trail shows the stage's true outcome in order:
        // started -> stage succeeded -> run revoked.
        List<AuditType> runEvents = env.store.auditTrail("svc").stream()
                .filter(e -> run.getRunId().equals(e.getRunId()))
                .map(AuditEvent::getType).toList();
        log.info("[REVOKE] audit sequence={}", runEvents);
        assertTrue(runEvents.indexOf(AuditType.RUN_STAGE_STARTED)
                < runEvents.indexOf(AuditType.RUN_STAGE_SUCCEEDED));
        assertTrue(runEvents.indexOf(AuditType.RUN_STAGE_SUCCEEDED)
                < runEvents.indexOf(AuditType.RUN_DEFINITION_REVOKED));

        // The withdrawn version never executes again; the kept stage result is not rewritten.
        assertThrows(Exception.class, () -> env.runs.advance(run.getRunId()));
        assertEquals(StageStatus.SUCCEEDED, env.runs.get(run.getRunId()).getStages().get(0).getStatus());
        assertEquals(1, stageCalls.get());
    }

    /** Waits until this thread is interrupted (the abort/revocation signal), then returns. */
    private static void awaitInterrupt() {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) {
            if (Thread.currentThread().isInterrupted()) {
                return;
            }
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                return; // interrupted: the revocation intent is now registered
            }
        }
        throw new IllegalStateException("stage was never interrupted within 10s");
    }

    @Test
    void withdrawSparesOtherVersionsOtherRunsAndReleaseHistory() {
        TestEnv env = TestEnv.create(new AlwaysSucceedStageHandler());
        DefinitionVersion v1 = env.definitions.createVersion("svc",
                definition("svc", 0, Duration.ZERO, List.of("dev"), "build"));
        PipelineRun good = env.runs.submit("svc", v1.getVersion(), "good", Map.of());
        env.runs.advance(good.getRunId());
        assertEquals(RunStatus.SUCCEEDED, env.runs.get(good.getRunId()).getStatus());
        ReleaseRecord release = env.releases.promote(good.getRunId());

        DefinitionVersion v2 = env.definitions.createVersion("svc",
                definition("svc", 0, Duration.ZERO, List.of("dev"), "build", "test"));
        PipelineRun doomed = env.runs.submit("svc", v2.getVersion(), "doomed", Map.of());

        env.definitions.withdraw("svc", v2.getVersion(), "v2 defective");

        // The v2 run is revoked; the v1 run, its success and the release history are intact.
        assertEquals(RunStatus.DEFINITION_REVOKED, env.runs.get(doomed.getRunId()).getStatus());
        assertEquals(RunStatus.SUCCEEDED, env.runs.get(good.getRunId()).getStatus());
        List<ReleaseRecord> history = env.releases.history("svc");
        assertEquals(1, history.size());
        assertEquals(ReleaseStatus.ACTIVE, history.get(0).getStatus());
        assertEquals(release.getReleaseId(), history.get(0).getReleaseId());
        log.info("[REVOKE] v2 revoked; v1 run and release {} untouched", release.getReleaseId());

        // Withdrawing v1 as well must not rewrite its terminal run or the published release.
        env.definitions.withdraw("svc", v1.getVersion(), "cleanup");
        assertEquals(RunStatus.SUCCEEDED, env.runs.get(good.getRunId()).getStatus());
        assertEquals(ReleaseStatus.ACTIVE, env.releases.history("svc").get(0).getStatus());
    }

    @Test
    void duplicateSubmitAroundRevocationStaysConsistent() {
        TestEnv env = TestEnv.create(new AlwaysSucceedStageHandler());
        DefinitionVersion v = env.definitions.createVersion("svc",
                definition("svc", 0, Duration.ZERO, List.of("dev"), "build"));
        PipelineRun run = env.runs.submit("svc", v.getVersion(), "dup-key", Map.of("x", "1"));

        env.definitions.withdraw("svc", v.getVersion(), "revoked");
        assertEquals(RunStatus.DEFINITION_REVOKED, env.runs.get(run.getRunId()).getStatus());

        // Re-submitting the same key targets the withdrawn version: rejected distinctly,
        // and crucially no second run is created for that key.
        DefinitionRejectedException ex = assertThrows(DefinitionRejectedException.class,
                () -> env.runs.submit("svc", v.getVersion(), "dup-key", Map.of("x", "1")));
        assertEquals(RejectReason.DEFINITION_WITHDRAWN, ex.getReason());
        assertEquals(1, env.store.listRuns("svc").size());
        PipelineRun after = env.runs.get(run.getRunId());
        assertEquals(RunStatus.DEFINITION_REVOKED, after.getStatus());
        assertEquals("dup-key", after.getIdempotencyKey());
        log.info("[REVOKE] duplicate submit rejected={}, runs={}", ex.getReason(),
                env.store.listRuns("svc").size());
    }
}
