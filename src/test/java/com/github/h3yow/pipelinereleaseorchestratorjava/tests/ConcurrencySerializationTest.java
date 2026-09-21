package com.github.h3yow.pipelinereleaseorchestratorjava.tests;

import com.github.h3yow.pipelinereleaseorchestratorjava.config.OrchestratorProperties;
import com.github.h3yow.pipelinereleaseorchestratorjava.error.CapacityRejectedException;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.DefinitionVersion;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.RejectPolicy;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.RunRecord;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.RunStatus;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.StageSpec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConcurrencySerializationTest extends HarnessCleanup {

    private static final Logger log = LoggerFactory.getLogger(ConcurrencySerializationTest.class);

    @TempDir
    Path tempDir;

    @Test
    void concurrentSubmissionsWithSameIdempotencyKeyCreateExactlyOneRun() throws Exception {
        TestHarness harness = new TestHarness(tempDir);
        DefinitionVersion v = harness.define("conc1", List.of("dev"),
                List.of(new StageSpec("build", true, List.of("dev"))));

        int threads = 16;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        ConcurrentLinkedQueue<String> runIds = new ConcurrentLinkedQueue<>();
        AtomicInteger conflicts = new AtomicInteger();
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    RunRecord run = harness.orchestrator
                            .submitRun("conc1", v.versionId(), "same-key", "same-payload");
                    runIds.add(run.runId());
                } catch (com.github.h3yow.pipelinereleaseorchestratorjava.error
                        .IdempotencyConflictException e) {
                    conflicts.incrementAndGet();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(20, TimeUnit.SECONDS));

        assertEquals(1, runIds.stream().distinct().count(), "all submitters see the same single run");
        assertEquals(0, conflicts.get(), "same key + same payload must never conflict");
        RunRecord done = harness.awaitTerminal(runIds.iterator().next());
        assertEquals(RunStatus.SUCCEEDED, done.status());
        assertEquals(1, harness.stageExecutor.executionCount("build", "dev"),
                "the run advances through its stages exactly once");
        log.info("concurrent-idempotent key=same-key version={} threads={} uniqueRuns=1 executions=1",
                v.versionId(), threads);
    }

    @Test
    void runsOfOnePipelineAreSerializedWithNoOutOfOrderStageExecution() throws Exception {
        TestHarness harness = new TestHarness(tempDir);
        DefinitionVersion v = harness.define("conc2", List.of("dev"), List.of(
                new StageSpec("build", true, List.of("dev")),
                new StageSpec("test", true, List.of("dev")),
                new StageSpec("deploy", true, List.of("dev"))));
        harness.stageExecutor.setStageDelayMillis(20);

        int n = 8;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(n);
        for (int i = 0; i < n; i++) {
            int idx = i;
            pool.submit(() -> {
                try {
                    start.await();
                    harness.orchestrator.submitRun("conc2", v.versionId(), "key-" + idx, "p");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(20, TimeUnit.SECONDS));

        for (RunRecord run : harness.orchestrator.runs("conc2")) {
            harness.awaitTerminal(run.runId());
        }
        for (RunRecord run : harness.orchestrator.runs("conc2")) {
            assertEquals(RunStatus.SUCCEEDED, run.status());
            assertEquals(3, run.stageExecutions().size(), "no skipped or duplicated stages");
            assertEquals(List.of("build", "test", "deploy"),
                    run.stageExecutions().stream().map(StageExecutionStage()).toList());
        }
        assertEquals(n, harness.stageExecutor.executionCount("build", "dev"));
        assertEquals(n, harness.stageExecutor.executionCount("test", "dev"));
        assertEquals(n, harness.stageExecutor.executionCount("deploy", "dev"));
        log.info("serialized-pipeline version={} runs={} eachAdvancedExactlyOnce", v.versionId(), n);
    }

    private static java.util.function.Function<
            com.github.h3yow.pipelinereleaseorchestratorjava.model.StageExecution, String>
            StageExecutionStage() {
        return com.github.h3yow.pipelinereleaseorchestratorjava.model.StageExecution::stage;
    }

    @Test
    void rejectPolicyRefusesBeyondConcurrencyLimitWithDistinguishableError() throws Exception {
        OrchestratorProperties props = TestHarness.defaultProperties();
        props.setMaxConcurrentRuns(1);
        props.setRejectPolicy(RejectPolicy.REJECT);
        TestHarness harness = new TestHarness(tempDir, props);
        DefinitionVersion v = harness.define("conc3", List.of("dev"),
                List.of(new StageSpec("build", true, List.of("dev"))));
        harness.stageExecutor.setStageDelayMillis(300);

        RunRecord first = harness.orchestrator.submitRun("conc3", v.versionId(), "c0", "p");
        Thread.sleep(50);
        // worker 1 busy -> REJECT policy refuses the next submission
        CapacityRejectedException ex = org.junit.jupiter.api.Assertions.assertThrows(
                CapacityRejectedException.class,
                () -> harness.orchestrator.submitRun("conc3", v.versionId(), "c1", "p"));
        log.info("capacity-rejected version={} busyRun={} reason={}", v.versionId(), first.runId(),
                ex.getMessage());
        harness.stageExecutor.setStageDelayMillis(0);
        harness.awaitTerminal(first.runId());
        // rejected submission must not have created a run or polluted history
        assertTrue(harness.orchestrator.runs("conc3").stream()
                .noneMatch(r -> "c1".equals(r.idempotencyKey())));
    }

    @Test
    void queuePolicyAcceptsBacklogUpToConfiguredCapacityThenRejects() throws Exception {
        OrchestratorProperties props = TestHarness.defaultProperties();
        props.setMaxConcurrentRuns(1);
        props.setQueueCapacity(1);
        props.setRejectPolicy(RejectPolicy.QUEUE);
        TestHarness harness = new TestHarness(tempDir, props);
        DefinitionVersion v = harness.define("conc4", List.of("dev"),
                List.of(new StageSpec("build", true, List.of("dev"))));
        harness.stageExecutor.setStageDelayMillis(300);

        RunRecord q0 = harness.orchestrator.submitRun("conc4", v.versionId(), "q0", "p");
        RunRecord q1 = harness.orchestrator.submitRun("conc4", v.versionId(), "q1", "p"); // queued
        // limit is threads(1)+queue(1)=2 in-flight; third must be rejected
        CapacityRejectedException ex = org.junit.jupiter.api.Assertions.assertThrows(
                CapacityRejectedException.class,
                () -> harness.orchestrator.submitRun("conc4", v.versionId(), "q2", "p"));
        log.info("queue-full-rejected version={} active={} queued=1 reason={}", v.versionId(),
                q0.runId(), ex.getMessage());

        harness.stageExecutor.setStageDelayMillis(0);
        harness.awaitTerminal(q0.runId());
        harness.awaitTerminal(q1.runId());
        assertEquals(2, harness.orchestrator.runs("conc4").size());
    }
}
