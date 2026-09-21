package com.github.h3yow.pipelinereleaseorchestratorjava.tests;

import com.github.h3yow.pipelinereleaseorchestratorjava.model.DefinitionVersion;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.PipelineRun;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.ReleaseRecord;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.ReleaseStatus;
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

import static com.github.h3yow.pipelinereleaseorchestratorjava.tests.TestEnv.definition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class IsolationTests {

    private static final Logger log = LoggerFactory.getLogger(IsolationTests.class);

    /** A failing run must not affect an already-succeeded run or the current release. */
    @Test
    void failureDoesNotPolluteExistingRunOrReleaseHistory() {
        // Fails only for the "bad" idempotency key, succeeds for everything else.
        StageHandler selective = (StageSpec stage, PipelineRun run, int attempt) ->
                "bad".equals(run.getIdempotencyKey())
                        ? StageResult.fail(false, "defective payload")
                        : StageResult.ok();
        TestEnv env = TestEnv.create(selective);
        DefinitionVersion v = env.definitions.createVersion("svc",
                definition("svc", 0, Duration.ZERO, List.of("prod"), "build"));

        PipelineRun good = env.runs.submit("svc", v.getVersion(), "good", Map.of());
        env.runs.advance(good.getRunId());
        ReleaseRecord release = env.releases.promote(good.getRunId());

        PipelineRun bad = env.runs.submit("svc", v.getVersion(), "bad", Map.of());
        PipelineRun badAfter = env.runs.advance(bad.getRunId());

        log.info("[ISOLATE] badRun status={} code={} | goodRun status={} currentRelease=v{} releases={}",
                badAfter.getStatus(), badAfter.getFailureCode(),
                env.runs.get(good.getRunId()).getStatus(),
                env.releases.currentRelease("svc", "prod").getDefinitionVersion(),
                env.releases.history("svc").size());

        assertEquals(RunStatus.FAILED_NON_RETRYABLE, badAfter.getStatus());
        // Existing successful run stays successful and remains the current ACTIVE release.
        assertEquals(RunStatus.SUCCEEDED, env.runs.get(good.getRunId()).getStatus());
        assertEquals(release.getReleaseId(),
                env.releases.currentRelease("svc", "prod").getReleaseId());
        assertEquals(ReleaseStatus.ACTIVE, release.getStatus());
        // The failed run produced no release row.
        assertEquals(1, env.releases.history("svc").size());
        // Both runs exist independently (failure did not overwrite the good run).
        assertEquals(2, env.store.listRuns("svc").size());
        assertNotNull(env.runs.get(bad.getRunId()).getFailureReason());
        assertNotNull(env.runs.get(bad.getRunId()).getFailureCode());
    }
}
