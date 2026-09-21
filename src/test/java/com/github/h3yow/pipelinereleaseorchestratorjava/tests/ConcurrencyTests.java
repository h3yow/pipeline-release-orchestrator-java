package com.github.h3yow.pipelinereleaseorchestratorjava.tests;

import com.github.h3yow.pipelinereleaseorchestratorjava.exception.OverflowException;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.DefinitionVersion;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.PipelineRun;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.RejectReason;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.h3yow.pipelinereleaseorchestratorjava.tests.TestEnv.definition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConcurrencyTests {

    private static final Logger log = LoggerFactory.getLogger(ConcurrencyTests.class);

    /** Concurrent advances of the same run must serialize; each stage executes exactly once. */
    @Test
    void concurrentAdvancesOfSameRunExecuteEveryStageExactlyOnce() throws Exception {
        Map<String, AtomicInteger> counters = new ConcurrentHashMap<>();
        counters.put("a", new AtomicInteger());
        counters.put("b", new AtomicInteger());
        StageHandler slow = (StageSpec stage, PipelineRun run, int attempt) -> {
            counters.get(stage.name()).incrementAndGet();
            Thread.sleep(20);
            return StageResult.ok();
        };
        TestEnv env = TestEnv.create(slow);
        DefinitionVersion v = env.definitions.createVersion("svc",
                definition("svc", 0, Duration.ZERO, List.of("dev"), "a", "b"));
        PipelineRun run = env.runs.submit("svc", v.getVersion(), "concurrent-advance", Map.of());

        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new java.util.ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                start.await();
                try {
                    env.runs.advance(run.getRunId());
                } catch (Exception ignored) {
                    // losing threads get ILLEGAL_TRANSITION; that must never cause double execute
                }
                return null;
            }));
        }
        start.countDown();
        for (Future<?> f : futures) {
            f.get(10, TimeUnit.SECONDS);
        }
        pool.shutdown();

        PipelineRun finalRun = env.runs.get(run.getRunId());
        log.info("[CONC] version=v{} finalStatus={} stageExecutions=a:{},b:{} stageRows={}",
                v.getVersion(), finalRun.getStatus(),
                counters.get("a").get(), counters.get("b").get(),
                finalRun.getStages().stream().map(s -> s.getName() + "=" + s.getStatus()).toList());

        assertEquals(RunStatus.SUCCEEDED, finalRun.getStatus());
        assertEquals(1, counters.get("a").get(), "stage a executed more than once");
        assertEquals(1, counters.get("b").get(), "stage b executed more than once");
    }

    /** REJECT policy: once in-flight slots are full, extra submits are rejected distinctly. */
    @Test
    void rejectPolicyRefusesBeyondInFlightCapWithDistinctReason() throws Exception {
        // Deterministic, lock-free-wait orchestration: cap=2, two runs are submitted (both
        // occupy in-flight slots). While they are non-terminal, overflow submits must be
        // rejected synchronously with OVERFLOW_REJECTED, regardless of lock scheduling.
        TestEnv env = TestEnv.create((stage, run, attempt) -> StageResult.ok(), "REJECT");
        DefinitionVersion v = env.definitions.createVersion("svc",
                definition("svc", 0, Duration.ZERO, List.of("dev"), "s"));

        PipelineRun r1 = env.runs.submit("svc", v.getVersion(), "cap-1", Map.of());
        PipelineRun r2 = env.runs.submit("svc", v.getVersion(), "cap-2", Map.of());
        assertEquals(2, env.store.countInFlight("svc"));

        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new java.util.ArrayList<>();
        AtomicInteger rejected = new AtomicInteger();
        AtomicInteger accepted = new AtomicInteger();
        for (int i = 0; i < threads; i++) {
            String key = "overflow-" + i;
            futures.add(pool.submit(() -> {
                start.await();
                try {
                    env.runs.submit("svc", v.getVersion(), key, Map.of());
                    accepted.incrementAndGet();
                } catch (OverflowException e) {
                    if (e.getReason() == RejectReason.OVERFLOW_REJECTED) {
                        rejected.incrementAndGet();
                    }
                }
                return null;
            }));
        }
        start.countDown();
        for (Future<?> f : futures) {
            f.get(10, TimeUnit.SECONDS);
        }
        pool.shutdown();

        log.info("[CONC] REJECT policy rejected={}/{} accepted={} (in-flight stayed {})",
                rejected.get(), threads, accepted.get(), env.store.countInFlight("svc"));
        assertEquals(threads, rejected.get());
        assertEquals(0, accepted.get());
        // Existing runs are not polluted by rejected submits.
        assertEquals(RunStatus.PENDING, env.runs.get(r1.getRunId()).getStatus());
        assertEquals(RunStatus.PENDING, env.runs.get(r2.getRunId()).getStatus());
        assertEquals(2, env.store.listRuns("svc").size());
    }

    /** QUEUE policy buffers excess runs (bounded) and drains FIFO via the pump. */
    @Test
    void queuePolicyBuffersAndDrainsFifo() {
        StageHandler noop = (stage, run, attempt) -> StageResult.ok();
        TestEnv env = TestEnv.create(noop, "QUEUE"); // 2 in-flight, 2 queued
        DefinitionVersion v = env.definitions.createVersion("svc",
                definition("svc", 0, Duration.ZERO, List.of("dev"), "s"));

        List<PipelineRun> submitted = new java.util.ArrayList<>();
        for (int i = 0; i < 4; i++) {
            submitted.add(env.runs.submit("svc", v.getVersion(), "q-" + i, Map.of()));
        }
        long queued = submitted.stream().filter(PipelineRun::isQueued).count();
        log.info("[CONC] QUEUE accepted 4: queuedFlag={} (first {} occupy running slots)",
                queued, env.properties.getMaxInFlightPerPipeline());
        assertEquals(env.properties.getMaxQueuedPerPipeline(), queued);

        // Fifth exceeds both running slots and queue buffer.
        OverflowException overflow = assertThrowsOverflow(
                () -> env.runs.submit("svc", v.getVersion(), "q-4", Map.of()));
        assertEquals(RejectReason.OVERFLOW_QUEUE_FULL, overflow.getReason());
        assertEquals(4, env.store.listRuns("svc").size());

        // Nothing drains queued runs while both slots stay occupied (first two PENDING).
        assertTrue(env.runs.pumpQueued("svc").isEmpty());

        // Free the two running slots by advancing them; pump now starts queued runs in FIFO.
        PipelineRun first = env.runs.advance(submitted.get(0).getRunId());
        PipelineRun second = env.runs.advance(submitted.get(1).getRunId());
        assertEquals(RunStatus.SUCCEEDED, first.getStatus());
        assertEquals(RunStatus.SUCCEEDED, second.getStatus());

        List<PipelineRun> started = env.runs.pumpQueued("svc");
        assertEquals(2, started.size());
        // FIFO: queued runs q-2, q-3 (in submission order).
        assertEquals(submitted.get(2).getRunId(), started.get(0).getRunId());
        assertEquals(submitted.get(3).getRunId(), started.get(1).getRunId());
        assertEquals(4, env.store.listRuns("svc").stream()
                .filter(r -> r.getStatus() == RunStatus.SUCCEEDED).count());
    }

    private static OverflowException assertThrowsOverflow(Runnable r) {
        try {
            r.run();
            throw new AssertionError("expected OverflowException");
        } catch (OverflowException e) {
            return e;
        }
    }
}
