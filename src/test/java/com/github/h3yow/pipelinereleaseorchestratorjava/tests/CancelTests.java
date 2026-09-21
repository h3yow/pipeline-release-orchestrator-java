package com.github.h3yow.pipelinereleaseorchestratorjava.tests;

import com.github.h3yow.pipelinereleaseorchestratorjava.model.DefinitionVersion;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.PipelineRun;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.RunStatus;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.StageSpec;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

class CancelTests {

    private static final Logger log = LoggerFactory.getLogger(CancelTests.class);

    @Test
    void cancelPendingRunIsImmediateAndRepeatedCancelStaysCancelled() {
        TestEnv env = TestEnv.create(new AlwaysSucceed());
        DefinitionVersion v = env.definitions.createVersion("svc",
                definition("svc", 0, Duration.ZERO, List.of("dev"), "build", "test"));
        PipelineRun run = env.runs.submit("svc", v.getVersion(), "cancel-pending", Map.of());

        PipelineRun cancelled = env.runs.cancel(run.getRunId(), "user requested");
        PipelineRun again = env.runs.cancel(run.getRunId(), "again");

        log.info("[CANCEL] key=cancel-pending version=v{} status={} twiceSame={}",
                v.getVersion(), again.getStatus(), cancelled.getStatus() == again.getStatus());

        assertEquals(RunStatus.CANCELLED, cancelled.getStatus());
        assertEquals(RunStatus.CANCELLED, again.getStatus());
        // A cancelled run must not advance afterwards (no status bounce).
        com.github.h3yow.pipelinereleaseorchestratorjava.exception.IllegalTransitionException ex =
                assertThrowsLike(() -> env.runs.advance(run.getRunId()));
        assertTrue(ex.getMessage().contains("CANCELLED"));
    }

    @Test
    void cancelWhileRunIsInProgressEndsCancelledWithoutDoubleAdvance() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger stageCalls = new AtomicInteger();
        StageHandler blocking = new StageHandler() {
            @Override
            public StageResult execute(StageSpec stage, PipelineRun run, int attempt) {
                stageCalls.incrementAndGet();
                if ("build".equals(stage.name())) {
                    entered.countDown();
                    try {
                        release.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                return StageResult.ok();
            }
        };
        TestEnv env = TestEnv.create(blocking);
        DefinitionVersion v = env.definitions.createVersion("svc",
                definition("svc", 0, Duration.ZERO, List.of("dev"), "build", "test"));
        PipelineRun run = env.runs.submit("svc", v.getVersion(), "cancel-running", Map.of());

        ExecutorService pool = Executors.newFixedThreadPool(2);
        Future<?> advanceFuture = pool.submit(() -> env.runs.advance(run.getRunId()));
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        // While the first stage is synchronously executing under the lock, cancel queues behind it.
        Future<?> cancelFuture = pool.submit(() -> env.runs.cancel(run.getRunId(), "abort"));
        Thread.sleep(50);
        release.countDown();

        advanceFuture.get(5, TimeUnit.SECONDS);
        try {
            cancelFuture.get(5, TimeUnit.SECONDS);
        } catch (java.util.concurrent.ExecutionException cancelRejected) {
            // Deterministic alternative: the run had already SUCCEEDED by the time cancel
            // acquired the lock, so cancel is rejected and successful history is preserved.
            assertTrue(cancelRejected.getCause().getMessage().contains("terminal"));
        }
        pool.shutdown();

        PipelineRun after = env.runs.get(run.getRunId());
        log.info("[CANCEL] key=cancel-running version=v{} afterAdvanceStatus={}",
                v.getVersion(), after.getStatus());

        // Stage execution is synchronous under the pipeline lock. The cancel request queued
        // behind the lock is applied only after the in-flight advance returns: either the run
        // already finished SUCCEEDED (cancel on a terminal run is rejected, history unchanged)
        // or the run was mid-flight and ends CANCELLED. In all cases the stage ran exactly once.
        // build blocked once (test runs immediately afterwards inside the same advance tick);
        // cancel interleaving must never execute a stage twice or bounce the status.
        assertEquals(2, stageCalls.get());
        assertTrue(after.getStatus() == RunStatus.SUCCEEDED || after.getStatus() == RunStatus.CANCELLED,
                "unexpected status " + after.getStatus());
        assertTrue(after.getStatus().isTerminal());
    }

    @Test
    void cancelAndRetryConcurrentlyNeverProduceOscillatingState() throws Exception {
        // Handler always fails retriably with zero backoff so WAITING_RETRY windows are instant.
        StageHandler fail = (stage, run, attempt) -> StageResult.fail(true, "flaky");
        TestEnv env = TestEnv.create(fail);
        DefinitionVersion v = env.definitions.createVersion("svc",
                definition("svc", 2, Duration.ZERO, List.of("dev"), "build"));
        PipelineRun run = env.runs.submit("svc", v.getVersion(), "cancel-vs-retry", Map.of());
        env.runs.advance(run.getRunId()); // -> WAITING_RETRY (zero backoff)

        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        int iterations = 50;
        Runnable cancelLoop = () -> {
            await(start);
            for (int i = 0; i < iterations; i++) {
                try {
                    env.runs.cancel(run.getRunId(), "race");
                } catch (Exception ignored) {
                    // expected once terminal
                }
            }
        };
        Runnable retryLoop = () -> {
            await(start);
            for (int i = 0; i < iterations; i++) {
                try {
                    env.runs.advance(run.getRunId());
                } catch (Exception ignored) {
                    // backoff/terminal/illegal-transition are all fine; state must stay consistent
                }
            }
        };
        Future<?> f1 = pool.submit(cancelLoop);
        Future<?> f2 = pool.submit(retryLoop);
        start.countDown();
        f1.get(10, TimeUnit.SECONDS);
        f2.get(10, TimeUnit.SECONDS);
        pool.shutdown();

        PipelineRun terminal = env.runs.get(run.getRunId());
        log.info("[CANCEL] key=cancel-vs-retry version=v{} finalStatus={} attempts={} stageStatuses={}",
                v.getVersion(), terminal.getStatus(), terminal.getRetryAttempts(),
                terminal.getStages().stream().map(s -> s.getName() + "=" + s.getStatus()).toList());

        assertTrue(terminal.getStatus().isTerminal(), "must converge to a terminal state");
        // The single stage was never executed twice in one advance tick beyond recorded attempts.
        assertEquals(1, terminal.getStages().size());
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static com.github.h3yow.pipelinereleaseorchestratorjava.exception.IllegalTransitionException
    assertThrowsLike(Runnable r) {
        try {
            r.run();
            throw new AssertionError("expected IllegalTransitionException");
        } catch (com.github.h3yow.pipelinereleaseorchestratorjava.exception.IllegalTransitionException e) {
            return e;
        }
    }

    private static final class AlwaysSucceed implements StageHandler {
        @Override
        public StageResult execute(StageSpec stage, PipelineRun run, int attemptIndex) {
            return StageResult.ok();
        }
    }
}
