package com.github.h3yow.pipelinereleaseorchestratorjava.tests;

import com.github.h3yow.pipelinereleaseorchestratorjava.error.IllegalRunStateException;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.DefinitionVersion;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.FailureReason;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.RunRecord;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.RunStatus;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.StageSpec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CancelConcurrencyTest extends HarnessCleanup {

    private static final Logger log = LoggerFactory.getLogger(CancelConcurrencyTest.class);

    @TempDir
    Path tempDir;

    @Test
    void cancelWhileStageIsRunningWinsAndStopsTheRun() throws Exception {
        TestHarness harness = new TestHarness(tempDir);
        DefinitionVersion v = harness.define("cancel1", List.of("dev"), List.of(
                new StageSpec("build", true, List.of("dev")),
                new StageSpec("deploy", true, List.of("dev"))));
        harness.stageExecutor.setStageDelayMillis(200);
        RunRecord run = harness.orchestrator.submitRun("cancel1", v.versionId(), "ck1", "p");

        Thread.sleep(40); // stage build is executing off-lock
        RunRecord cancelled = harness.orchestrator.cancel(run.runId());
        log.info("cancel-during-stage key=ck1 version={} run={} status=CANCELLED", v.versionId(),
                run.runId());
        assertEquals(RunStatus.CANCELLED, cancelled.status());

        RunRecord terminal = harness.awaitTerminal(run.runId());
        assertEquals(RunStatus.CANCELLED, terminal.status());
        assertEquals(FailureReason.CANCELLED, terminal.failureReason());
        Thread.sleep(250); // let the in-flight (result-discarded) stage return
        assertEquals(0, harness.stageExecutor.executionCount("deploy", "dev"),
                "no stage after a cancelled stage may execute");
        assertEquals(1, harness.stageExecutor.executionCount("build", "dev"),
                "the in-flight stage executed exactly once but its result was discarded");
    }

    @Test
    void cancelDuringBackoffPreventsTheAutomaticRetry() throws Exception {
        TestHarness harness = new TestHarness(tempDir);
        DefinitionVersion v = harness.define("cancel2", List.of("dev"), List.of(
                new StageSpec("build", true, List.of("dev"))));
        harness.stageExecutor.alwaysFail("build", "dev");

        RunRecord run = harness.orchestrator.submitRun("cancel2", v.versionId(), "ck2", "p");
        // wait until the worker is sleeping after the first failed attempt
        harness.awaitStatus(run.runId(), RunStatus.FAILED_RETRYABLE);
        RunRecord cancelled = harness.orchestrator.cancel(run.runId());
        log.info("cancel-during-backoff key=ck2 version={} run={} status=CANCELLED", v.versionId(),
                run.runId());
        assertEquals(RunStatus.CANCELLED, cancelled.status());

        Thread.sleep(200); // let any would-be auto retry fire
        RunRecord settled = harness.orchestrator.getRun(run.runId());
        assertEquals(RunStatus.CANCELLED, settled.status(), "state must not bounce back");
        assertEquals(1, harness.stageExecutor.executionCount("build", "dev"),
                "auto retry must not execute after cancellation");
        assertThrows(IllegalRunStateException.class,
                () -> harness.orchestrator.retry(run.runId()));
    }

    @Test
    void cancellingATerminalRunIsRejectedAndDoesNotChangeHistory() throws Exception {
        TestHarness harness = new TestHarness(tempDir);
        DefinitionVersion v = harness.define("cancel3", List.of("dev"), List.of(
                new StageSpec("build", true, List.of("dev"))));
        RunRecord run = harness.orchestrator.submitRun("cancel3", v.versionId(), "ck3", "p");
        harness.awaitTerminal(run.runId());

        assertThrows(IllegalRunStateException.class,
                () -> harness.orchestrator.cancel(run.runId()));
        assertEquals(RunStatus.SUCCEEDED, harness.orchestrator.getRun(run.runId()).status());
        log.info("cancel-terminal rejected key=ck3 version={} run={} status=SUCCEEDED",
                v.versionId(), run.runId());
    }

    @Test
    void racingCancelsProduceExactlyOneCancelledTransition() throws Exception {
        TestHarness harness = new TestHarness(tempDir);
        DefinitionVersion v = harness.define("cancel4", List.of("dev"), List.of(
                new StageSpec("build", true, List.of("dev")),
                new StageSpec("deploy", true, List.of("dev"))));
        harness.stageExecutor.setStageDelayMillis(150);
        RunRecord run = harness.orchestrator.submitRun("cancel4", v.versionId(), "ck4", "p");
        Thread.sleep(30);

        java.util.concurrent.atomic.AtomicInteger okCount =
                new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicInteger rejectCount =
                new java.util.concurrent.atomic.AtomicInteger();
        Runnable cancelTask = () -> {
            try {
                harness.orchestrator.cancel(run.runId());
                okCount.incrementAndGet();
            } catch (IllegalRunStateException e) {
                rejectCount.incrementAndGet();
            }
        };
        Thread t1 = new Thread(cancelTask);
        Thread t2 = new Thread(cancelTask);
        t1.start();
        t2.start();
        t1.join();
        t2.join();

        assertEquals(1, okCount.get(), "exactly one cancel may succeed");
        assertEquals(1, rejectCount.get(), "the racing cancel must be rejected");
        RunRecord terminal = harness.awaitTerminal(run.runId());
        assertEquals(RunStatus.CANCELLED, terminal.status());
        long cancelEvents = harness.auditLog.events("cancel4").stream()
                .filter(e -> e.type() == com.github.h3yow.pipelinereleaseorchestratorjava.model.EventType.RUN_CANCELLED)
                .count();
        assertEquals(1, cancelEvents, "no duplicated transitions in the audit trail");
        log.info("racing-cancels key=ck4 version={} run={} accepted=1 rejected=1", v.versionId(),
                run.runId());
    }
}
