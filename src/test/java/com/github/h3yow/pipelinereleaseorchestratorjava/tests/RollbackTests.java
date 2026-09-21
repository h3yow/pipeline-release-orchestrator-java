package com.github.h3yow.pipelinereleaseorchestratorjava.tests;

import com.github.h3yow.pipelinereleaseorchestratorjava.exception.IllegalTransitionException;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.AuditEvent;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.AuditType;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.DefinitionVersion;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.PipelineRun;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.ReleaseRecord;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.ReleaseStatus;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.RejectReason;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.stage.AlwaysSucceedStageHandler;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static com.github.h3yow.pipelinereleaseorchestratorjava.tests.TestEnv.definition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RollbackTests {

    private static final Logger log = LoggerFactory.getLogger(RollbackTests.class);

    private PipelineRun succeed(TestEnv env, String pipeline, int version, String key) {
        PipelineRun run = env.runs.submit(pipeline, version, key, Map.of());
        return env.runs.advance(run.getRunId());
    }

    @Test
    void rollbackReactivatesPriorReleaseAndPreservesCompleteOrderedHistory() {
        TestEnv env = TestEnv.create(new AlwaysSucceedStageHandler());
        DefinitionVersion v1 = env.definitions.createVersion("svc",
                definition("svc", 0, Duration.ZERO, List.of("prod"), "build"));
        DefinitionVersion v2 = env.definitions.createVersion("svc",
                definition("svc", 0, Duration.ZERO, List.of("prod"), "build", "test"));

        ReleaseRecord relV1 = env.releases.promote(succeed(env, "svc", v1.getVersion(), "rel-v1").getRunId());
        ReleaseRecord relV2 = env.releases.promote(succeed(env, "svc", v2.getVersion(), "rel-v2").getRunId());

        assertEquals(v2.getVersion(), env.releases.currentRelease("svc", "prod").getDefinitionVersion());
        assertEquals(ReleaseStatus.SUPERSEDED, relV1.getStatus());
        assertEquals(relV2.getReleaseId(), relV1.getSupersededByReleaseId());

        // Roll back the CURRENT (v2, still ACTIVE/not superseded) to the prior v1 release.
        ReleaseRecord restored = env.releases.rollback("svc", "prod",
                relV1.getReleaseId(), "hotfix regression");

        log.info("[ROLLBACK] after rollback current=v{} relV2.status={} relV2.rolledBackTo={} relV1.status={}",
                env.releases.currentRelease("svc", "prod").getDefinitionVersion(),
                relV2.getStatus(), relV2.getRolledBackToReleaseId(), relV1.getStatus());

        assertEquals(relV1.getReleaseId(), restored.getReleaseId());
        assertEquals(v1.getVersion(), env.releases.currentRelease("svc", "prod").getDefinitionVersion());
        assertEquals(ReleaseStatus.ACTIVE, relV1.getStatus());
        assertEquals(ReleaseStatus.ROLLED_BACK, relV2.getStatus());
        assertEquals(relV1.getReleaseId(), relV2.getRolledBackToReleaseId());
        assertEquals("hotfix regression", relV2.getRollbackReason());
        assertNotNull(relV2.getRolledBackAt());

        // History rows are never dropped and their sequence stays explainable.
        List<ReleaseRecord> history = env.releases.history("svc");
        assertEquals(2, history.size());
        assertEquals(List.of(v1.getVersion(), v2.getVersion()),
                history.stream().map(ReleaseRecord::getDefinitionVersion).toList());

        List<AuditEvent> trail = env.store.auditTrail("svc");
        List<AuditType> types = trail.stream().map(AuditEvent::getType).toList();
        log.info("[ROLLBACK] audit sequence={}", types);
        assertTrue(types.contains(AuditType.RELEASE_SUPERSEDED));
        assertTrue(types.contains(AuditType.RELEASE_ROLLED_BACK));
        AuditEvent rb = trail.stream().filter(e -> e.getType() == AuditType.RELEASE_ROLLED_BACK)
                .findFirst().orElseThrow();
        assertEquals(relV2.getReleaseId(), rb.getReleaseId());
        assertTrue(rb.getReason().contains("hotfix regression"));
    }

    @Test
    void invalidRollbackTargetsAreRejectedWithDistinctReasons() {
        TestEnv env = TestEnv.create(new AlwaysSucceedStageHandler());
        DefinitionVersion v1 = env.definitions.createVersion("svc",
                definition("svc", 0, Duration.ZERO, List.of("prod"), "build"));
        DefinitionVersion v2 = env.definitions.createVersion("svc",
                definition("svc", 0, Duration.ZERO, List.of("prod"), "build", "test"));
        DefinitionVersion vOther = env.definitions.createVersion("other",
                definition("other", 0, Duration.ZERO, List.of("prod"), "build"));

        ReleaseRecord relV1 = env.releases.promote(succeed(env, "svc", v1.getVersion(), "k1").getRunId());
        ReleaseRecord relV2 = env.releases.promote(succeed(env, "svc", v2.getVersion(), "k2").getRunId());
        ReleaseRecord otherRel = env.releases.promote(
                succeed(env, "other", vOther.getVersion(), "ko").getRunId());

        // Unknown id
        assertEquals(RejectReason.RELEASE_NOT_FOUND,
                assertThrows(IllegalTransitionException.class,
                        () -> env.releases.rollback("svc", "prod", "does-not-exist", "x")).getReason());
        // Belongs to a different pipeline/environment
        assertEquals(RejectReason.RELEASE_NOT_FOUND,
                assertThrows(IllegalTransitionException.class,
                        () -> env.releases.rollback("svc", "prod", otherRel.getReleaseId(), "x")).getReason());
        // Target is the current release itself
        assertEquals(RejectReason.RELEASE_NOT_ACTIVE,
                assertThrows(IllegalTransitionException.class,
                        () -> env.releases.rollback("svc", "prod", relV2.getReleaseId(), "self")).getReason());

        // After rolling back v2 -> v1, v2 is dead: using it as a target is rejected.
        env.releases.rollback("svc", "prod", relV1.getReleaseId(), "back");
        IllegalTransitionException dead = assertThrows(IllegalTransitionException.class,
                () -> env.releases.rollback("svc", "prod", relV2.getReleaseId(), "again"));
        log.info("[ROLLBACK] dead target rejected reason={}", dead.getReason());
        assertEquals(RejectReason.RELEASE_NOT_ACTIVE, dead.getReason());
    }

    @Test
    void rollbackWithoutAnyReleaseIsRejected() {
        TestEnv env = TestEnv.create(new AlwaysSucceedStageHandler());
        IllegalTransitionException ex = assertThrows(IllegalTransitionException.class,
                () -> env.releases.rollback("svc", "prod", "x", "none"));
        assertEquals(RejectReason.RELEASE_NOT_FOUND, ex.getReason());
    }
}
