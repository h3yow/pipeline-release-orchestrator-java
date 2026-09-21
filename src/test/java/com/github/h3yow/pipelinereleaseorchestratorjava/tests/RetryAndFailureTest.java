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
import static org.junit.jupiter.api.Assertions.assertTrue;

class RetryAndFailureTest extends HarnessCleanup {

    private static final Logger log = LoggerFactory.getLogger(RetryAndFailureTest.class);

    @TempDir
    Path tempDir;

    @Test
    void retryableFailureRetriesAutomaticallyWithConfiguredPolicyAndThenSucceeds() throws Exception {
        TestHarness harness = new TestHarness(tempDir);
        DefinitionVersion v = harness.define("retry1", List.of("dev"), List.of(
                new StageSpec("build", true, List.of("dev"))));
        harness.stageExecutor.failAttempts("build", "dev", attempt -> attempt <= 2);

        RunRecord run = harness.orchestrator.submitRun("retry1", v.versionId(), "rk1", "p");
        RunRecord done = harness.awaitTerminal(run.runId());

        assertEquals(RunStatus.SUCCEEDED, done.status());
        assertEquals(3, harness.stageExecutor.executionCount("build", "dev"),
                "two failed attempts plus one successful attempt");
        log.info("auto-retry-success key=rk1 version={} run={} status=SUCCEEDED executions={}",
                v.versionId(), run.runId(), harness.stageExecutor.executionCount("build", "dev"));
    }

    @Test
    void nonRetryableStageFailsImmediatelyAndKeepsTheOriginalReason() throws Exception {
        TestHarness harness = new TestHarness(tempDir);
        DefinitionVersion v = harness.define("retry2", List.of("dev"), List.of(
                new StageSpec("build", false, List.of("dev")))); // non-retryable stage
        harness.stageExecutor.alwaysFail("build", "dev");

        RunRecord run = harness.orchestrator.submitRun("retry2", v.versionId(), "rk2", "p");
        RunRecord done = harness.awaitTerminal(run.runId());

        assertEquals(RunStatus.FAILED_FATAL, done.status());
        assertEquals(FailureReason.STAGE_FAILED_FATAL, done.failureReason());
        assertTrue(done.failureMessage().contains("scripted failure on attempt 1"),
                "original failure message must be retained");
        assertEquals(1, harness.stageExecutor.executionCount("build", "dev"),
                "non-retryable failure must not be retried");
        log.info("fatal-termination key=rk2 version={} run={} status=FAILED_FATAL reason={} message={}",
                v.versionId(), run.runId(), done.failureReason(), done.failureMessage());
    }

    @Test
    void explicitlyFatalResultTerminatesEvenForRetryableStage() throws Exception {
        TestHarness harness = new TestHarness(tempDir);
        DefinitionVersion v = harness.define("retry3", List.of("dev"), List.of(
                new StageSpec("build", true, List.of("dev"))));
        harness.stageExecutor.fatalAttempts("build", "dev", attempt -> true);

        RunRecord run = harness.orchestrator.submitRun("retry3", v.versionId(), "rk3", "p");
        RunRecord done = harness.awaitTerminal(run.runId());
        assertEquals(RunStatus.FAILED_FATAL, done.status());
        assertEquals(FailureReason.STAGE_FAILED_FATAL, done.failureReason());
        assertEquals(1, harness.stageExecutor.executionCount("build", "dev"));
        log.info("explicit-fatal key=rk3 version={} run={} reason={}", v.versionId(), run.runId(),
                done.failureReason());
    }

    @Test
    void exhaustedAutomaticRetriesAllowManualRetryAndSucceed() throws Exception {
        TestHarness harness = new TestHarness(tempDir);
        DefinitionVersion v = harness.define("retry4", List.of("dev"), List.of(
                new StageSpec("build", true, List.of("dev"))));
        // fail all automatic attempts (maxAttempts=3), then succeed once the script is changed
        harness.stageExecutor.failAttempts("build", "dev", attempt -> true);

        RunRecord run = harness.orchestrator.submitRun("retry4", v.versionId(), "rk4", "p");
        RunRecord exhausted = harness.awaitFailureReason(run.runId(), FailureReason.RETRIES_EXHAUSTED);
        assertEquals(FailureReason.RETRIES_EXHAUSTED, exhausted.failureReason());
        assertEquals(3, harness.stageExecutor.executionCount("build", "dev"));
        log.info("retries-exhausted key=rk4 version={} run={} status=FAILED_RETRYABLE reason={}",
                v.versionId(), run.runId(), exhausted.failureReason());

        // fix the failure and retry manually -> success; retry on SUCCEEDED is rejected
        harness.stageExecutor.failAttempts("build", "dev", attempt -> false);
        harness.orchestrator.retry(run.runId());
        RunRecord done = harness.awaitTerminal(run.runId());
        assertEquals(RunStatus.SUCCEEDED, done.status());
        assertThrows(IllegalRunStateException.class, () -> harness.orchestrator.retry(run.runId()));
        log.info("manual-retry-success key=rk4 version={} run={} status=SUCCEEDED", v.versionId(),
                run.runId());
    }
}
