package com.github.h3yow.pipelinereleaseorchestratorjava.tests;

import com.github.h3yow.pipelinereleaseorchestratorjava.exception.IllegalTransitionException;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.DefinitionVersion;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.PipelineRun;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.ReleaseRecord;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.RejectReason;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.RunStatus;
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

import static com.github.h3yow.pipelinereleaseorchestratorjava.tests.TestEnv.definition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PromotionTests {

    private static final Logger log = LoggerFactory.getLogger(PromotionTests.class);

    @Test
    void promotionFollowsDeclaredEnvironmentOrderWithoutSkipOrDuplicate() {
        TestEnv env = TestEnv.create(new AlwaysSucceedStageHandler());
        DefinitionVersion v = env.definitions.createVersion("svc",
                definition("svc", 0, Duration.ZERO, List.of("dev", "staging", "prod"), "build"));
        PipelineRun run = env.runs.submit("svc", v.getVersion(), "promo-1", Map.of());
        env.runs.advance(run.getRunId());

        ReleaseRecord dev = env.releases.promote(run.getRunId());
        ReleaseRecord staging = env.releases.promote(run.getRunId());

        log.info("[PROMO] runId={} version=v{} promotedEnvs={}",
                run.getRunId(), v.getVersion(), run.getPromotedEnvironments());

        assertEquals("dev", dev.getEnvironment());
        assertEquals("staging", staging.getEnvironment());
        assertEquals(List.of("dev", "staging"), run.getPromotedEnvironments());

        // The third promotion goes to prod; a fourth is a duplicate and must be rejected.
        ReleaseRecord prod = env.releases.promote(run.getRunId());
        assertEquals("prod", prod.getEnvironment());
        IllegalTransitionException exhausted = assertThrows(IllegalTransitionException.class,
                () -> env.releases.promote(run.getRunId()));
        log.info("[PROMO] duplicate promotion rejected reason={}", exhausted.getReason());
        assertEquals(RejectReason.ENV_ALREADY_PROMOTED, exhausted.getReason());
    }

    @Test
    void failedRunCanNeverBePromotedOrJudgedSuccessful() {
        StageHandler failBuild = (stage, run, attempt) ->
                StageResult.fail(false, "compile error");
        TestEnv env = TestEnv.create(failBuild);
        DefinitionVersion v = env.definitions.createVersion("svc",
                definition("svc", 3, Duration.ofMillis(1), List.of("dev"), "build", "test"));
        PipelineRun run = env.runs.submit("svc", v.getVersion(), "promo-2", Map.of());
        PipelineRun after = env.runs.advance(run.getRunId());

        log.info("[PROMO] runId={} version=v{} status={} failureCode={} reason={}",
                run.getRunId(), v.getVersion(), after.getStatus(), after.getFailureCode(),
                after.getFailureReason());

        assertEquals(RunStatus.FAILED_NON_RETRYABLE, after.getStatus());
        assertEquals(StageStatus.FAILED, after.getStages().get(0).getStatus());
        assertEquals(StageStatus.PENDING, after.getStages().get(1).getStatus());

        IllegalTransitionException ex = assertThrows(IllegalTransitionException.class,
                () -> env.releases.promote(run.getRunId()));
        assertEquals(RejectReason.ENV_NO_ELIGIBLE_RUN, ex.getReason());
    }
}
