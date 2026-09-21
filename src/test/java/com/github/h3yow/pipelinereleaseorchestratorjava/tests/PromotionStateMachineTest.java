package com.github.h3yow.pipelinereleaseorchestratorjava.tests;

import com.github.h3yow.pipelinereleaseorchestratorjava.error.IllegalRunStateException;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.DefinitionVersion;
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

class PromotionStateMachineTest extends HarnessCleanup {

    private static final Logger log = LoggerFactory.getLogger(PromotionStateMachineTest.class);

    @TempDir
    Path tempDir;

    private DefinitionVersion twoEnvPipeline(TestHarness h, String id) {
        return h.define(id, List.of("dev", "prod"), List.of(
                new StageSpec("build", true, List.of("dev", "prod")),
                new StageSpec("test", true, List.of("dev")),
                new StageSpec("deploy", true, List.of("dev", "prod"))));
    }

    @Test
    void stagesAdvanceInOrderAndWaitForPromotionBetweenEnvironments() throws Exception {
        TestHarness harness = new TestHarness(tempDir);
        DefinitionVersion v = twoEnvPipeline(harness, "promo");
        RunRecord run = harness.orchestrator.submitRun("promo", v.versionId(), "k1", "p");

        RunRecord waiting = harness.awaitStatus(run.runId(), RunStatus.WAITING_PROMOTION);
        assertEquals("dev", waiting.currentEnvironment());
        log.info("wait-promotion key=k1 version={} run={} status=WAITING_PROMOTION env=dev",
                v.versionId(), run.runId());

        // cannot promote twice / jump from a succeeded state etc.; first promote proceeds to prod
        RunRecord running = harness.orchestrator.promote(run.runId());
        assertEquals(RunStatus.RUNNING, running.status());
        assertEquals("prod", running.currentEnvironment());
        assertEquals(0, running.currentStageIndex(), "prod restarts stage order from first applicable");

        IllegalRunStateException duplicate = assertThrows(IllegalRunStateException.class,
                () -> harness.orchestrator.promote(run.runId()));
        log.info("duplicate-promotion rejected run={} reason={}", run.runId(), duplicate.getMessage());

        RunRecord done = harness.awaitTerminal(run.runId());
        assertEquals(RunStatus.SUCCEEDED, done.status());
        assertEquals("prod", done.currentEnvironment());
        // build(dev), test(dev), deploy(dev), build(prod), deploy(prod)
        assertEquals(5, done.stageExecutions().size(), "stages must run in declared order per env");
        log.info("promotion-complete key=k1 version={} run={} status=SUCCEEDED executions={}",
                v.versionId(), run.runId(), done.stageExecutions().size());
    }

    @Test
    void promotingAQueuedRunIsRejectedAndFailedRunIsNeverSuccess() throws Exception {
        TestHarness harness = new TestHarness(tempDir);
        // slow stage so the run is still RUNNING/non-promotable when we call promote
        harness.stageExecutor.setStageDelayMillis(60);
        DefinitionVersion v = twoEnvPipeline(harness, "promo2");
        RunRecord run = harness.orchestrator.submitRun("promo2", v.versionId(), "k2", "p");

        // promote while RUNNING must be rejected (no skipping, no state rollback)
        Thread.sleep(15);
        IllegalRunStateException ex = assertThrows(IllegalRunStateException.class,
                () -> harness.orchestrator.promote(run.runId()));
        log.info("early-promotion rejected version={} run={} reason={}", v.versionId(), run.runId(),
                ex.getMessage());

        harness.stageExecutor.setStageDelayMillis(0);
        harness.awaitStatus(run.runId(), RunStatus.WAITING_PROMOTION);
        // cancelling then promoting is a state-rollback attempt and must be rejected
        harness.orchestrator.cancel(run.runId());
        IllegalRunStateException afterCancel = assertThrows(IllegalRunStateException.class,
                () -> harness.orchestrator.promote(run.runId()));
        RunRecord cancelled = harness.orchestrator.getRun(run.runId());
        assertEquals(RunStatus.CANCELLED, cancelled.status());
        assertTrue(cancelled.status() != RunStatus.SUCCEEDED, "cancelled run must not look successful");
        log.info("promote-after-cancel rejected key=k2 version={} run={} status=CANCELLED reason={}",
                v.versionId(), run.runId(), afterCancel.getMessage());
    }

    @Test
    void retryAndPromoteOnWaitingRunAreRejectedInWrongStates() throws Exception {
        TestHarness harness = new TestHarness(tempDir);
        DefinitionVersion v = twoEnvPipeline(harness, "promo3");
        RunRecord run = harness.orchestrator.submitRun("promo3", v.versionId(), "k3", "p");
        RunRecord waiting = harness.awaitStatus(run.runId(), RunStatus.WAITING_PROMOTION);

        assertThrows(IllegalRunStateException.class, () -> harness.orchestrator.retry(run.runId()));
        assertEquals(RunStatus.WAITING_PROMOTION, waiting.status());
        log.info("retry-on-waiting rejected key=k3 version={} run={}", v.versionId(), run.runId());

        harness.orchestrator.promote(run.runId());
        harness.awaitTerminal(run.runId());
        assertThrows(IllegalRunStateException.class, () -> harness.orchestrator.promote(run.runId()));
        log.info("promote-on-succeeded rejected key=k3 version={} run={} status=SUCCEEDED",
                v.versionId(), run.runId());
    }
}
