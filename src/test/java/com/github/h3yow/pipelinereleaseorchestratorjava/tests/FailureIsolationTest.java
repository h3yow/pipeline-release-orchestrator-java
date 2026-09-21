package com.github.h3yow.pipelinereleaseorchestratorjava.tests;

import com.github.h3yow.pipelinereleaseorchestratorjava.model.DefinitionVersion;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.FailureReason;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.ReleaseRecord;
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

class FailureIsolationTest extends HarnessCleanup {

    private static final Logger log = LoggerFactory.getLogger(FailureIsolationTest.class);

    @TempDir
    Path tempDir;

    @Test
    void failedRunNeitherPublishesNorPollutesPriorRunsAndReleases() throws Exception {
        TestHarness harness = new TestHarness(tempDir);
        List<StageSpec> stages = List.of(new StageSpec("build", false, List.of("dev")));
        DefinitionVersion v = harness.define("iso", List.of("dev"), stages);

        // first run succeeds and creates the first release
        RunRecord good = harness.orchestrator.submitRun("iso", v.versionId(), "good", "p");
        harness.awaitTerminal(good.runId());
        assertEquals(RunStatus.SUCCEEDED, harness.orchestrator.getRun(good.runId()).status());
        List<ReleaseRecord> before = harness.releaseService.history("iso", "dev");
        assertEquals(1, before.size());

        // second run fails fatally
        harness.stageExecutor.alwaysFail("build", "dev");
        RunRecord bad = harness.orchestrator.submitRun("iso", v.versionId(), "bad", "p");
        RunRecord failed = harness.awaitTerminal(bad.runId());
        assertEquals(RunStatus.FAILED_FATAL, failed.status());
        assertEquals(FailureReason.STAGE_FAILED_FATAL, failed.failureReason());
        log.info("failed-run isolated key=bad version={} run={} reason={} message={}", v.versionId(),
                bad.runId(), failed.failureReason(), failed.failureMessage());

        // release history untouched; first run still SUCCEEDED
        List<ReleaseRecord> after = harness.releaseService.history("iso", "dev");
        assertEquals(before.size(), after.size(), "a failed run must not create/supersede releases");
        assertEquals(before.get(0).releaseId(), after.get(0).releaseId());
        assertEquals(RunStatus.SUCCEEDED, harness.orchestrator.getRun(good.runId()).status());
        log.info("history-clean version={} releases={} goodRunStatus=SUCCEEDED badRunStatus=FAILED_FATAL",
                v.versionId(), after.size());
    }

    @Test
    void retryExhaustedRunKeepsLastReasonAndDoesNotPublish() throws Exception {
        TestHarness harness = new TestHarness(tempDir);
        DefinitionVersion v = harness.define("iso2", List.of("dev"),
                List.of(new StageSpec("build", true, List.of("dev"))));
        harness.stageExecutor.alwaysFail("build", "dev");

        RunRecord run = harness.orchestrator.submitRun("iso2", v.versionId(), "exhaust", "p");
        RunRecord failed = harness.awaitFailureReason(run.runId(), FailureReason.RETRIES_EXHAUSTED);
        assertEquals(RunStatus.FAILED_RETRYABLE, failed.status());
        assertEquals(0, harness.releaseService.history("iso2", "dev").size());
        log.info("exhausted-no-release key=exhaust version={} run={} reason={}", v.versionId(),
                run.runId(), failed.failureReason());
    }
}
