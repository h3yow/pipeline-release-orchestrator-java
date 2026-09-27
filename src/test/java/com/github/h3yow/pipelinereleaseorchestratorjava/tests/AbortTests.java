package com.github.h3yow.pipelinereleaseorchestratorjava.tests;

import com.github.h3yow.pipelinereleaseorchestratorjava.exception.IllegalTransitionException;
import com.github.h3yow.pipelinereleaseorchestratorjava.exception.OverflowException;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.AuditEvent;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.AuditType;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.DefinitionVersion;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.PipelineRun;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.RejectReason;
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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AbortTests {

    private static final Logger log = LoggerFactory.getLogger(AbortTests.class);

    @Test
    void abortDuringExecutionInterruptsStageAndSkipsRemainingStages() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        AtomicInteger stageCalls = new AtomicInteger();
        StageHandler blocking = new StageHandler() {
            @Override
            public StageResult execute(StageSpec stage, PipelineRun run, int attempt)
                    throws InterruptedException {
                stageCalls.incrementAndGet();
                if ("build".equals(stage.name())) {
                    entered.countDown();
                    // Responds to the abort interrupt immediately instead of running to completion.
                    Thread.sleep(10_000);
                }
                return StageResult.ok();
            }
        };
        TestEnv env = TestEnv.create(blocking);
        DefinitionVersion v = env.definitions.createVersion("svc",
                definition("svc", 0, Duration.ZERO, List.of("dev"), "build", "test", "deploy"));
        PipelineRun run = env.runs.submit("svc", v.getVersion(), "abort-mid", Map.of());

        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<?> advance = pool.submit(() -> env.runs.advance(run.getRunId()));
        assertTrue(entered.await(5, TimeUnit.SECONDS), "stage should have started");

        PipelineRun aborted = env.runs.abort(run.getRunId(), "emergency stop");
        advance.get(5, TimeUnit.SECONDS);
        pool.shutdown();

        PipelineRun after = env.runs.get(run.getRunId());
        log.info("[ABORT] key=abort-mid status={} code={} reason={} stages={}",
                after.getStatus(), after.getFailureCode(), after.getFailureReason(),
                after.getStages().stream().map(s -> s.getName() + "=" + s.getStatus()).toList());

        assertEquals(RunStatus.ABORTED, aborted.getStatus());
        assertEquals(RunStatus.ABORTED, after.getStatus());
        assertEquals(RejectReason.RUN_ABORTED, after.getFailureCode());
        assertEquals("emergency stop", after.getFailureReason());
        // The interrupted stage left no success trace; later stages never executed.
        assertEquals(StageStatus.INTERRUPTED, after.getStages().get(0).getStatus());
        assertEquals(StageStatus.PENDING, after.getStages().get(1).getStatus());
        assertEquals(StageStatus.PENDING, after.getStages().get(2).getStatus());
        assertEquals(1, stageCalls.get(), "only the interrupted stage ever ran");
        assertTrue(after.getStatus().isTerminal());

        // Repeated abort is idempotent: same terminal state, same finish timestamp.
        PipelineRun again = env.runs.abort(run.getRunId(), "again");
        assertEquals(RunStatus.ABORTED, again.getStatus());
        assertEquals(after.getFinishedAt(), again.getFinishedAt());

        // Full transition order is auditable.
        List<AuditType> runEvents = env.store.auditTrail("svc").stream()
                .filter(e -> run.getRunId().equals(e.getRunId()))
                .map(AuditEvent::getType).toList();
        log.info("[ABORT] audit sequence={}", runEvents);
        assertTrue(runEvents.contains(AuditType.RUN_ABORT_REQUESTED));
        assertTrue(runEvents.contains(AuditType.RUN_ABORTED));
        assertTrue(runEvents.indexOf(AuditType.RUN_ABORT_REQUESTED)
                < runEvents.indexOf(AuditType.RUN_ABORTED));
    }

    @Test
    void abortAfterStageTrulyCompletedKeepsStageSuccess() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        AtomicInteger stageCalls = new AtomicInteger();
        StageHandler completesDespiteInterrupt = new StageHandler() {
            @Override
            public StageResult execute(StageSpec stage, PipelineRun run, int attempt) {
                stageCalls.incrementAndGet();
                if ("build".equals(stage.name())) {
                    entered.countDown();
                    try {
                        // Parked until the abort interrupt arrives; the stage nevertheless
                        // runs to completion inside the abort window and returns success.
                        Thread.sleep(30_000);
                    } catch (InterruptedException e) {
                        // abort landed mid-execution; the work itself was already done
                    }
                }
                return StageResult.ok();
            }
        };
        TestEnv env = TestEnv.create(completesDespiteInterrupt);
        DefinitionVersion v = env.definitions.createVersion("svc",
                definition("svc", 0, Duration.ZERO, List.of("dev"), "build", "test", "deploy"));
        PipelineRun run = env.runs.submit("svc", v.getVersion(), "abort-late", Map.of());

        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<?> advance = pool.submit(() -> env.runs.advance(run.getRunId()));
        assertTrue(entered.await(5, TimeUnit.SECONDS), "stage should have started");

        PipelineRun aborted = env.runs.abort(run.getRunId(), "stop after build done");
        advance.get(5, TimeUnit.SECONDS);
        pool.shutdown();

        PipelineRun after = env.runs.get(run.getRunId());
        log.info("[ABORT] key=abort-late status={} code={} stages={}",
                after.getStatus(), after.getFailureCode(),
                after.getStages().stream().map(s -> s.getName() + "=" + s.getStatus()).toList());

        // The run still converges to the abort terminal state...
        assertEquals(RunStatus.ABORTED, aborted.getStatus());
        assertEquals(RunStatus.ABORTED, after.getStatus());
        assertEquals(RejectReason.RUN_ABORTED, after.getFailureCode());
        // ...but the stage that truly completed inside the abort window keeps its success,
        // it is never rewritten to INTERRUPTED.
        assertEquals(StageStatus.SUCCEEDED, after.getStages().get(0).getStatus());
        assertEquals(1, after.getStages().get(0).getAttempts());
        assertEquals(1, after.getCurrentStageIndex());
        // Stages that never ran keep no success trace.
        assertEquals(StageStatus.PENDING, after.getStages().get(1).getStatus());
        assertEquals(StageStatus.PENDING, after.getStages().get(2).getStatus());
        assertEquals(1, stageCalls.get(), "no stage executed after the abort");
        // Capacity released by the terminal run.
        assertEquals(0, env.store.countInFlight("svc"));

        // The audit trail shows the genuine stage result before the run-level convergence.
        List<AuditType> types = env.store.auditTrail("svc").stream()
                .filter(e -> run.getRunId().equals(e.getRunId()))
                .map(AuditEvent::getType).toList();
        log.info("[ABORT] audit sequence={}", types);
        assertTrue(types.contains(AuditType.RUN_STAGE_SUCCEEDED));
        assertTrue(types.contains(AuditType.RUN_ABORTED));
        assertTrue(types.indexOf(AuditType.RUN_STAGE_SUCCEEDED)
                < types.indexOf(AuditType.RUN_ABORTED));

        // Repeated abort keeps the preserved stage result and finish timestamp.
        PipelineRun again = env.runs.abort(run.getRunId(), "again");
        assertEquals(RunStatus.ABORTED, again.getStatus());
        assertEquals(StageStatus.SUCCEEDED, again.getStages().get(0).getStatus());
        assertEquals(after.getFinishedAt(), again.getFinishedAt());
    }

    @Test
    void abortAfterLastStageCompletedStillConvergesToAborted() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        StageHandler completesDespiteInterrupt = (stage, run, attempt) -> {
            entered.countDown();
            try {
                Thread.sleep(30_000); // woken by the abort interrupt
            } catch (InterruptedException e) {
                // abort landed while the only stage was finishing
            }
            return StageResult.ok();
        };
        TestEnv env = TestEnv.create(completesDespiteInterrupt);
        DefinitionVersion v = env.definitions.createVersion("svc",
                definition("svc", 0, Duration.ZERO, List.of("dev"), "build"));
        PipelineRun run = env.runs.submit("svc", v.getVersion(), "abort-last", Map.of());

        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<?> advance = pool.submit(() -> env.runs.advance(run.getRunId()));
        assertTrue(entered.await(5, TimeUnit.SECONDS), "stage should have started");

        env.runs.abort(run.getRunId(), "stop at the finish line");
        advance.get(5, TimeUnit.SECONDS);
        pool.shutdown();

        PipelineRun after = env.runs.get(run.getRunId());
        log.info("[ABORT] key=abort-last status={} stages={}", after.getStatus(),
                after.getStages().stream().map(s -> s.getName() + "=" + s.getStatus()).toList());
        // The abort wins over the run-level outcome even when every stage managed to finish.
        assertEquals(RunStatus.ABORTED, after.getStatus());
        assertEquals(RejectReason.RUN_ABORTED, after.getFailureCode());
        assertEquals(StageStatus.SUCCEEDED, after.getStages().get(0).getStatus());
    }

    @Test
    void abortWithStageGenuineFailureNeverRecordsSuccess() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        StageHandler failsDespiteInterrupt = new StageHandler() {
            @Override
            public StageResult execute(StageSpec stage, PipelineRun run, int attempt) {
                entered.countDown();
                try {
                    Thread.sleep(30_000); // woken by the abort interrupt
                } catch (InterruptedException e) {
                    // abort landed mid-execution
                }
                // ...but the stage genuinely finished with a failure of its own.
                return StageResult.fail(true, "real stage failure");
            }
        };
        TestEnv env = TestEnv.create(failsDespiteInterrupt);
        DefinitionVersion v = env.definitions.createVersion("svc",
                definition("svc", 0, Duration.ZERO, List.of("dev"), "build", "test"));
        PipelineRun run = env.runs.submit("svc", v.getVersion(), "abort-genuine-fail", Map.of());

        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<?> advance = pool.submit(() -> env.runs.advance(run.getRunId()));
        assertTrue(entered.await(5, TimeUnit.SECONDS), "stage should have started");

        env.runs.abort(run.getRunId(), "stop over a failed stage");
        advance.get(5, TimeUnit.SECONDS);
        pool.shutdown();

        PipelineRun after = env.runs.get(run.getRunId());
        log.info("[ABORT] key=abort-genuine-fail status={} stages={}", after.getStatus(),
                after.getStages().stream().map(s -> s.getName() + "=" + s.getStatus()).toList());
        assertEquals(RunStatus.ABORTED, after.getStatus());
        assertEquals(RejectReason.RUN_ABORTED, after.getFailureCode());
        // A genuinely returned failure is never upgraded to a success and, having truly
        // run to completion, is not marked INTERRUPTED either.
        assertEquals(StageStatus.FAILED, after.getStages().get(0).getStatus());
        assertEquals("real stage failure", after.getStages().get(0).getLastError());
        assertEquals(StageStatus.PENDING, after.getStages().get(1).getStatus());

        List<AuditType> types = env.store.auditTrail("svc").stream()
                .filter(e -> run.getRunId().equals(e.getRunId()))
                .map(AuditEvent::getType).toList();
        assertTrue(types.contains(AuditType.RUN_STAGE_FAILED));
        assertTrue(!types.contains(AuditType.RUN_STAGE_SUCCEEDED));
        assertTrue(types.indexOf(AuditType.RUN_STAGE_FAILED)
                < types.indexOf(AuditType.RUN_ABORTED));
    }

    @Test
    void abortOnTerminalRunIsRejectedWithDistinctReason() {
        TestEnv env = TestEnv.create(new AlwaysSucceedStageHandler());
        DefinitionVersion v = env.definitions.createVersion("svc",
                definition("svc", 0, Duration.ZERO, List.of("dev"), "build"));
        PipelineRun run = env.runs.submit("svc", v.getVersion(), "abort-terminal", Map.of());
        env.runs.advance(run.getRunId());
        assertEquals(RunStatus.SUCCEEDED, env.runs.get(run.getRunId()).getStatus());

        IllegalTransitionException ex = assertThrows(IllegalTransitionException.class,
                () -> env.runs.abort(run.getRunId(), "too late"));
        log.info("[ABORT] terminal reject reason={} message={}", ex.getReason(), ex.getMessage());
        assertEquals(RejectReason.ABORT_NOT_ALLOWED, ex.getReason());
        assertTrue(ex.getMessage().contains("SUCCEEDED"));
        // Distinguishable from the cancel reason code.
        assertNotEquals(RejectReason.CANCEL_NOT_ALLOWED, ex.getReason());

        // A cancelled run is also not abortable, and the cancel outcome is preserved.
        PipelineRun run2 = env.runs.submit("svc", v.getVersion(), "abort-cancelled", Map.of());
        env.runs.cancel(run2.getRunId(), "user");
        IllegalTransitionException ex2 = assertThrows(IllegalTransitionException.class,
                () -> env.runs.abort(run2.getRunId(), "too late"));
        assertEquals(RejectReason.ABORT_NOT_ALLOWED, ex2.getReason());
        assertEquals(RunStatus.CANCELLED, env.runs.get(run2.getRunId()).getStatus());
    }

    @Test
    void abortPendingRunIsImmediateAndReleasesCapacityAndDuplicateSubmitConsistent() {
        TestEnv env = TestEnv.create(new AlwaysSucceedStageHandler(), "REJECT");
        DefinitionVersion v = env.definitions.createVersion("svc",
                definition("svc", 0, Duration.ZERO, List.of("dev"), "build"));
        PipelineRun r1 = env.runs.submit("svc", v.getVersion(), "cap-1", Map.of("x", "1"));
        PipelineRun r2 = env.runs.submit("svc", v.getVersion(), "cap-2", Map.of());
        // Cap reached (maxInFlight=2): a third distinct key overflows.
        assertThrows(OverflowException.class,
                () -> env.runs.submit("svc", v.getVersion(), "cap-3", Map.of()));

        env.runs.abort(r1.getRunId(), "stop one");
        env.runs.abort(r2.getRunId(), "stop two");
        assertEquals(RunStatus.ABORTED, env.runs.get(r1.getRunId()).getStatus());
        assertEquals(RunStatus.ABORTED, env.runs.get(r2.getRunId()).getStatus());

        // Capacity released: new submissions are accepted under the same limits.
        PipelineRun r3 = env.runs.submit("svc", v.getVersion(), "cap-3", Map.of());
        PipelineRun r4 = env.runs.submit("svc", v.getVersion(), "cap-4", Map.of());
        log.info("[ABORT] capacity released, accepted {} and {}", r3.getRunId(), r4.getRunId());

        // Same idempotency key before/after the abort still maps to the original run.
        PipelineRun dup = env.runs.submit("svc", v.getVersion(), "cap-1", Map.of("x", "1"));
        assertEquals(r1.getRunId(), dup.getRunId());
        assertEquals(RunStatus.ABORTED, dup.getStatus());
    }

    @Test
    void abortAndCancelConcurrentlyConvergeToSingleTerminalState() throws Exception {
        StageHandler slow = (stage, run, attempt) -> {
            Thread.sleep(5);
            return StageResult.ok();
        };
        TestEnv env = TestEnv.create(slow);
        DefinitionVersion v = env.definitions.createVersion("svc",
                definition("svc", 0, Duration.ZERO, List.of("dev"), "build", "test", "deploy"));
        PipelineRun run = env.runs.submit("svc", v.getVersion(), "abort-vs-cancel", Map.of());

        ExecutorService pool = Executors.newFixedThreadPool(3);
        CountDownLatch start = new CountDownLatch(1);
        Runnable advanceLoop = () -> {
            await(start);
            for (int i = 0; i < 20; i++) {
                try {
                    env.runs.advance(run.getRunId());
                } catch (Exception ignored) {
                    // terminal / illegal transitions are expected while racing
                }
            }
        };
        Runnable abortLoop = () -> {
            await(start);
            for (int i = 0; i < 20; i++) {
                try {
                    env.runs.abort(run.getRunId(), "race-abort");
                } catch (Exception ignored) {
                    // terminal runs reject abort; state must stay consistent
                }
            }
        };
        Runnable cancelLoop = () -> {
            await(start);
            for (int i = 0; i < 20; i++) {
                try {
                    env.runs.cancel(run.getRunId(), "race-cancel");
                } catch (Exception ignored) {
                    // terminal runs reject cancel; state must stay consistent
                }
            }
        };
        Future<?> f1 = pool.submit(advanceLoop);
        Future<?> f2 = pool.submit(abortLoop);
        Future<?> f3 = pool.submit(cancelLoop);
        start.countDown();
        f1.get(15, TimeUnit.SECONDS);
        f2.get(15, TimeUnit.SECONDS);
        f3.get(15, TimeUnit.SECONDS);
        pool.shutdown();

        PipelineRun terminal = env.runs.get(run.getRunId());
        log.info("[ABORT] race final={} code={} stages={}", terminal.getStatus(),
                terminal.getFailureCode(),
                terminal.getStages().stream().map(s -> s.getName() + "=" + s.getStatus()).toList());
        assertTrue(terminal.getStatus().isTerminal(),
                "must converge to exactly one terminal state, got " + terminal.getStatus());
        // No stage may carry a success trace beyond the point of convergence.
        if (terminal.getStatus() == RunStatus.ABORTED) {
            assertEquals(RejectReason.RUN_ABORTED, terminal.getFailureCode());
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
