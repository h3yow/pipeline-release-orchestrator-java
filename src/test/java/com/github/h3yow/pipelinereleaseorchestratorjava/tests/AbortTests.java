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
import com.github.h3yow.pipelinereleaseorchestratorjava.service.stage.StageHandler;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.stage.StageResult;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Immediate-abort semantics for in-flight runs. */
class AbortTests {

    private static final Logger log = LoggerFactory.getLogger(AbortTests.class);

    @Test
    void abortDuringExecutionInterruptsStageAndNeverRunsLaterStages() throws Exception {
        CountDownLatch buildEntered = new CountDownLatch(1);
        AtomicInteger stageCalls = new AtomicInteger();
        // "build" blocks until the abort interrupts the executing thread.
        StageHandler blocking = (StageSpec stage, PipelineRun run, int attempt) -> {
            stageCalls.incrementAndGet();
            if ("build".equals(stage.name())) {
                buildEntered.countDown();
                try {
                    new CountDownLatch(1).await(); // never released; abort must interrupt
                } catch (InterruptedException e) {
                    throw e; // surfaced as a failed result; abort handling takes over
                }
            }
            return StageResult.ok();
        };
        TestEnv env = TestEnv.create(blocking);
        DefinitionVersion v = env.definitions.createVersion("svc",
                definition("svc", 0, Duration.ZERO, List.of("dev"), "build", "test", "deploy"));
        PipelineRun run = env.runs.submit("svc", v.getVersion(), "abort-mid", Map.of());

        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<?> advance = pool.submit(() -> env.runs.advance(run.getRunId()));
        assertTrue(buildEntered.await(5, TimeUnit.SECONDS), "stage should be executing");

        PipelineRun aborted = env.runs.abort(run.getRunId(), "hotfix needed now");
        advance.get(5, TimeUnit.SECONDS);
        pool.shutdown();

        log.info("[ABORT] key=abort-mid status={} stages={}", aborted.getStatus(),
                aborted.getStages().stream().map(s -> s.getName() + "=" + s.getStatus()).toList());

        assertEquals(RunStatus.ABORTED, aborted.getStatus());
        assertTrue(aborted.getStatus().isTerminal());
        // The interrupted stage is marked ABORTED; later stages never executed (no success trace).
        assertEquals(StageStatus.ABORTED, aborted.getStages().get(0).getStatus());
        assertEquals(StageStatus.PENDING, aborted.getStages().get(1).getStatus());
        assertEquals(StageStatus.PENDING, aborted.getStages().get(2).getStatus());
        assertEquals(0, aborted.getStages().get(1).getAttempts());
        assertEquals(0, aborted.getStages().get(2).getAttempts());
        assertEquals(1, stageCalls.get(), "only the first stage ever executed");
        // The abort trigger reason is readable from the run detail.
        assertEquals(RejectReason.RUN_ABORTED, aborted.getFailureCode());
        assertTrue(aborted.getFailureReason().contains("hotfix needed now"));
        assertNotNull(aborted.getFinishedAt());
        // The audit trail shows the abort in the transition sequence.
        List<AuditEvent> trail = env.store.auditTrail("svc");
        assertTrue(trail.stream().anyMatch(e -> e.getType() == AuditType.RUN_ABORTED
                && e.getToStatus() == RunStatus.ABORTED));
        assertTrue(trail.stream().noneMatch(e -> e.getType() == AuditType.RUN_STAGE_SUCCEEDED),
                "no stage may be recorded successful");
        // A terminal run cannot advance any more (no status bounce).
        assertThrows(IllegalTransitionException.class, () -> env.runs.advance(run.getRunId()));
    }

    @Test
    void abortPendingAndWaitingRetryRunsConvergesImmediatelyAndStaysAborted() {
        // First: a retryable failure puts the run into WAITING_RETRY.
        StageHandler flakyOnce = new StageHandler() {
            private final AtomicInteger calls = new AtomicInteger();

            @Override
            public StageResult execute(StageSpec stage, PipelineRun run, int attempt) {
                return calls.incrementAndGet() == 1
                        ? StageResult.fail(true, "transient") : StageResult.ok();
            }
        };
        TestEnv env = TestEnv.create(flakyOnce);
        DefinitionVersion v = env.definitions.createVersion("svc",
                definition("svc", 3, Duration.ofMinutes(5), List.of("dev"), "build", "test"));
        PipelineRun waiting = env.runs.submit("svc", v.getVersion(), "abort-waiting", Map.of());
        env.runs.advance(waiting.getRunId());
        assertEquals(RunStatus.WAITING_RETRY, env.runs.get(waiting.getRunId()).getStatus());

        PipelineRun abortedWaiting = env.runs.abort(waiting.getRunId(), "stop retrying");
        assertEquals(RunStatus.ABORTED, abortedWaiting.getStatus());
        assertEquals(RejectReason.RUN_ABORTED, abortedWaiting.getFailureCode());
        // The failed stage record is preserved as evidence, not rewritten.
        assertEquals(StageStatus.FAILED, abortedWaiting.getStages().get(0).getStatus());
        assertEquals("transient", abortedWaiting.getStages().get(0).getLastError());

        // Second: a plain PENDING run aborts immediately too.
        PipelineRun pending = env.runs.submit("svc", v.getVersion(), "abort-pending", Map.of());
        PipelineRun abortedPending = env.runs.abort(pending.getRunId(), "not needed");
        assertEquals(RunStatus.ABORTED, abortedPending.getStatus());
        assertEquals(StageStatus.PENDING, abortedPending.getStages().get(0).getStatus());

        // Repeated aborts are idempotent: same terminal state, same finish instant.
        Instant finished = abortedPending.getFinishedAt();
        PipelineRun again = env.runs.abort(pending.getRunId(), "again");
        PipelineRun third = env.runs.abort(pending.getRunId(), "and again");
        log.info("[ABORT] key=abort-pending repeated={} {}", again.getStatus(), third.getStatus());
        assertEquals(RunStatus.ABORTED, again.getStatus());
        assertEquals(RunStatus.ABORTED, third.getStatus());
        assertEquals(finished, again.getFinishedAt());
        assertEquals(finished, third.getFinishedAt());
    }

    @Test
    void abortOnFinishedRunIsRejectedWithDistinctReason() {
        TestEnv env = TestEnv.create((stage, run, attempt) -> StageResult.ok());
        DefinitionVersion v = env.definitions.createVersion("svc",
                definition("svc", 0, Duration.ZERO, List.of("dev"), "build"));
        PipelineRun run = env.runs.submit("svc", v.getVersion(), "abort-late", Map.of());
        env.runs.advance(run.getRunId());
        assertEquals(RunStatus.SUCCEEDED, env.runs.get(run.getRunId()).getStatus());

        IllegalTransitionException ex = assertThrows(IllegalTransitionException.class,
                () -> env.runs.abort(run.getRunId(), "too late"));
        log.info("[ABORT] key=abort-late rejected reason={} msg={}", ex.getReason(), ex.getMessage());
        // Distinguishable from cancel-on-terminal and from generic illegal transitions.
        assertEquals(RejectReason.ABORT_NOT_ALLOWED, ex.getReason());
        // The successful run is untouched by the rejected abort.
        PipelineRun after = env.runs.get(run.getRunId());
        assertEquals(RunStatus.SUCCEEDED, after.getStatus());
        assertTrue(!after.isAbortRequested());
    }

    @Test
    void abortReleasesInFlightCapacityAndDuplicateSubmitStaysConsistent() {
        // Always-retryable failure with a long backoff keeps runs in WAITING_RETRY:
        // they hold in-flight slots without holding the pipeline lock.
        StageHandler flaky = (stage, run, attempt) -> StageResult.fail(true, "transient");
        TestEnv env = TestEnv.create(flaky); // REJECT policy, maxInFlight=2
        DefinitionVersion v = env.definitions.createVersion("svc",
                definition("svc", 10, Duration.ofMinutes(10), List.of("dev"), "build", "test"));
        PipelineRun r1 = env.runs.submit("svc", v.getVersion(), "cap-1", Map.of());
        PipelineRun r2 = env.runs.submit("svc", v.getVersion(), "cap-2", Map.of());
        env.runs.advance(r1.getRunId());
        env.runs.advance(r2.getRunId());
        assertEquals(RunStatus.WAITING_RETRY, env.runs.get(r1.getRunId()).getStatus());
        assertEquals(RunStatus.WAITING_RETRY, env.runs.get(r2.getRunId()).getStatus());

        // Cap reached (both slots held): a third submission is rejected.
        assertThrows(OverflowException.class,
                () -> env.runs.submit("svc", v.getVersion(), "cap-3", Map.of()));

        // Aborting one run frees its slot immediately; the next submit is accepted.
        env.runs.abort(r1.getRunId(), "free the slot");
        PipelineRun r3 = env.runs.submit("svc", v.getVersion(), "cap-3", Map.of());
        assertEquals(RunStatus.PENDING, r3.getStatus());

        // Same idempotency key before/after the abort still maps to the same run.
        PipelineRun dup = env.runs.submit("svc", v.getVersion(), "cap-1", Map.of());
        log.info("[ABORT] key=cap-1 dupRunId={} status={}", dup.getRunId(), dup.getStatus());
        assertEquals(r1.getRunId(), dup.getRunId());
        assertEquals(RunStatus.ABORTED, dup.getStatus());

        // Clean up the remaining runs; every abort converges to the same terminal state.
        env.runs.abort(r2.getRunId(), "cleanup");
        env.runs.abort(r3.getRunId(), "cleanup");

        assertEquals(RunStatus.ABORTED, env.runs.get(r1.getRunId()).getStatus());
        assertEquals(RunStatus.ABORTED, env.runs.get(r2.getRunId()).getStatus());
        assertEquals(RunStatus.ABORTED, env.runs.get(r3.getRunId()).getStatus());
        // Capacity fully released: fresh submissions are accepted again.
        PipelineRun r4 = env.runs.submit("svc", v.getVersion(), "cap-4", Map.of());
        assertEquals(RunStatus.PENDING, r4.getStatus());
        PipelineRun r5 = env.runs.submit("svc", v.getVersion(), "cap-5", Map.of());
        assertEquals(RunStatus.PENDING, r5.getStatus());
    }

    @Test
    void abortAndCancelConcurrentlyConvergeToSingleTerminalState() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        StageHandler blocking = (StageSpec stage, PipelineRun run, int attempt) -> {
            if ("build".equals(stage.name())) {
                entered.countDown();
                try {
                    new CountDownLatch(1).await();
                } catch (InterruptedException e) {
                    throw e;
                }
            }
            return StageResult.ok();
        };
        TestEnv env = TestEnv.create(blocking);
        DefinitionVersion v = env.definitions.createVersion("svc",
                definition("svc", 0, Duration.ZERO, List.of("dev"), "build", "test"));
        PipelineRun run = env.runs.submit("svc", v.getVersion(), "abort-vs-cancel", Map.of());

        ExecutorService pool = Executors.newFixedThreadPool(3);
        Future<?> advance = pool.submit(() -> env.runs.advance(run.getRunId()));
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        // Abort interrupts the stage; cancel queues behind the lock. No oscillation allowed.
        Future<?> abort = pool.submit(() -> env.runs.abort(run.getRunId(), "kill"));
        Future<?> cancel = pool.submit(() -> {
            try {
                env.runs.cancel(run.getRunId(), "cancel");
            } catch (IllegalTransitionException expected) {
                // run already ABORTED by the time cancel acquired the lock
            }
        });
        advance.get(5, TimeUnit.SECONDS);
        abort.get(5, TimeUnit.SECONDS);
        cancel.get(5, TimeUnit.SECONDS);
        pool.shutdown();

        PipelineRun after = env.runs.get(run.getRunId());
        log.info("[ABORT] key=abort-vs-cancel final={} stages={}", after.getStatus(),
                after.getStages().stream().map(s -> s.getName() + "=" + s.getStatus()).toList());
        // Abort interrupted the in-flight stage, so ABORTED is the deterministic outcome.
        assertEquals(RunStatus.ABORTED, after.getStatus());
        assertTrue(after.getStatus().isTerminal());
        assertEquals(StageStatus.PENDING, after.getStages().get(1).getStatus());
        // Exactly one terminal transition in the audit trail (no bounce, no half update).
        long terminalTransitions = env.store.auditTrail("svc").stream()
                .filter(e -> e.getRunId() != null && e.getRunId().equals(run.getRunId()))
                .filter(e -> e.getToStatus() != null && e.getToStatus().isTerminal())
                .count();
        assertEquals(1, terminalTransitions);
    }
}
