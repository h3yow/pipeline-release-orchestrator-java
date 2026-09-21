package com.github.h3yow.pipelinereleaseorchestratorjava.tests;

import com.github.h3yow.pipelinereleaseorchestratorjava.error.RollbackNotAllowedException;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.AuditEvent;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.DefinitionVersion;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.EventType;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.ReleaseRecord;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.ReleaseStatus;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.ReleaseType;
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

class RollbackAndAuditTest extends HarnessCleanup {

    private static final Logger log = LoggerFactory.getLogger(RollbackAndAuditTest.class);

    @TempDir
    Path tempDir;

    private DefinitionVersion pipeline(TestHarness h, String id, List<StageSpec> stages) {
        return h.define(id, List.of("dev"), stages);
    }

    @Test
    void successfulRunPublishesReleaseAndSupersedesThePreviousOne() throws Exception {
        TestHarness harness = new TestHarness(tempDir);
        DefinitionVersion v1 = pipeline(harness, "rel1",
                List.of(new StageSpec("build", true, List.of("dev"))));
        RunRecord run1 = harness.orchestrator.submitRun("rel1", v1.versionId(), "rkey1", "p1");
        harness.awaitTerminal(run1.runId());
        ReleaseRecord first = harness.releaseService.history("rel1", "dev").get(0);
        assertEquals(ReleaseStatus.ACTIVE, first.status());
        assertEquals(ReleaseType.DEPLOY, first.type());
        log.info("release-published version={} release={} status=ACTIVE", v1.versionId(),
                first.releaseId());

        DefinitionVersion v2 = pipeline(harness, "rel1",
                List.of(new StageSpec("build", true, List.of("dev")),
                        new StageSpec("verify", true, List.of("dev"))));
        RunRecord run2 = harness.orchestrator.submitRun("rel1", v2.versionId(), "rkey2", "p2");
        harness.awaitTerminal(run2.runId());

        List<ReleaseRecord> history = harness.releaseService.history("rel1", "dev");
        assertEquals(2, history.size());
        assertEquals(ReleaseStatus.SUPERSEDED, history.get(0).status(),
                "older release must be marked superseded but retained");
        ReleaseRecord active = history.get(1);
        assertEquals(ReleaseStatus.ACTIVE, active.status());
        assertEquals(v2.versionId(), active.definitionVersion());
        log.info("supersede version={} activeRelease={} oldRelease=SUPERSEDED", v2.versionId(),
                active.releaseId());
    }

    @Test
    void rollbackActsOnlyOnActiveReleaseRestoresPriorVersionAndKeepsHistory() throws Exception {
        TestHarness harness = new TestHarness(tempDir);
        DefinitionVersion v1 = pipeline(harness, "rel2",
                List.of(new StageSpec("build", true, List.of("dev"))));
        harness.awaitTerminal(harness.orchestrator
                .submitRun("rel2", v1.versionId(), "a1", "p").runId());
        ReleaseRecord release1 = harness.releaseService.history("rel2", "dev").get(0);

        DefinitionVersion v2 = pipeline(harness, "rel2",
                List.of(new StageSpec("build", true, List.of("dev")),
                        new StageSpec("verify", true, List.of("dev"))));
        harness.awaitTerminal(harness.orchestrator
                .submitRun("rel2", v2.versionId(), "a2", "p").runId());
        List<ReleaseRecord> historyAfterSecond = harness.releaseService.history("rel2", "dev");
        ReleaseRecord release1Superseded = historyAfterSecond.get(0);
        ReleaseRecord release2 = historyAfterSecond.get(1);
        assertEquals(release1.releaseId(), release1Superseded.releaseId());
        assertEquals(ReleaseStatus.SUPERSEDED, release1Superseded.status());
        assertEquals(ReleaseStatus.ACTIVE, release2.status());

        // rolling back a superseded release id is rejected
        RollbackNotAllowedException stale = assertThrows(RollbackNotAllowedException.class,
                () -> harness.releaseService.rollback("rel2", "dev", release1.releaseId(), null));
        log.info("rollback-superseded rejected target={} reason={}", release1.releaseId(),
                stale.getMessage());

        // rolling back the currently active release restores v1 as a new ROLLBACK entry
        ReleaseRecord rolledBack = harness.releaseService
                .rollback("rel2", "dev", release2.releaseId(), "manual");
        assertEquals(ReleaseType.ROLLBACK, rolledBack.type());
        assertEquals(ReleaseStatus.ACTIVE, rolledBack.status());
        assertEquals(v1.versionId(), rolledBack.definitionVersion());
        assertEquals(release2.releaseId(), rolledBack.relatedReleaseId());
        log.info("rollback version restoredVersion={} rollbackRelease={} related={}", v1.versionId(),
                rolledBack.releaseId(), rolledBack.relatedReleaseId());

        // full history retained and ordered: r1(SUPERSEDED), r2(SUPERSEDED), rollback(ACTIVE)
        List<ReleaseRecord> history = harness.releaseService.history("rel2", "dev");
        assertEquals(3, history.size());
        assertEquals(release1.releaseId(), history.get(0).releaseId());
        assertEquals(release2.releaseId(), history.get(1).releaseId());
        assertEquals(rolledBack.releaseId(), history.get(2).releaseId());
        assertEquals(ReleaseStatus.ACTIVE, history.get(2).status());
        assertTrue(harness.releaseService.history("rel2", "dev").stream()
                        .anyMatch(r -> r.definitionVersion().equals(v2.versionId())),
                "the rolled-back v2 release must not be discarded");

        // rolling back again from the new active release restores v2 (immediate predecessor)
        ReleaseRecord secondRollback = harness.releaseService
                .rollback("rel2", "dev", rolledBack.releaseId(), null);
        assertEquals(v2.versionId(), secondRollback.definitionVersion());
        assertEquals(4, harness.releaseService.history("rel2", "dev").size());
        log.info("second-rollback restoredVersion={} release={}", v2.versionId(),
                secondRollback.releaseId());
    }

    @Test
    void rollbackOfFirstReleaseAndUnknownReleaseAreRejected() throws Exception {
        TestHarness harness = new TestHarness(tempDir);
        DefinitionVersion v1 = pipeline(harness, "rel3",
                List.of(new StageSpec("build", true, List.of("dev"))));
        harness.awaitTerminal(harness.orchestrator
                .submitRun("rel3", v1.versionId(), "a3", "p").runId());
        ReleaseRecord only = harness.releaseService.history("rel3", "dev").get(0);

        assertThrows(RollbackNotAllowedException.class,
                () -> harness.releaseService.rollback("rel3", "dev", "rel-does-not-exist", null));
        RollbackNotAllowedException noPredecessor = assertThrows(RollbackNotAllowedException.class,
                () -> harness.releaseService.rollback("rel3", "dev", only.releaseId(), null));
        log.info("rollback-no-predecessor rejected release={} reason={}", only.releaseId(),
                noPredecessor.getMessage());
    }

    @Test
    void auditTrailIsOrderedAndCarriesKeyVersionAndTransitions() throws Exception {
        TestHarness harness = new TestHarness(tempDir);
        DefinitionVersion v = pipeline(harness, "rel4",
                List.of(new StageSpec("build", true, List.of("dev"))));
        RunRecord run = harness.orchestrator.submitRun("rel4", v.versionId(), "audit-key", "p");
        harness.awaitTerminal(run.runId());
        assertEquals(RunStatus.SUCCEEDED, harness.orchestrator.getRun(run.runId()).status());

        List<AuditEvent> events = harness.auditLog.events("rel4");
        long distinctSeqs = events.stream().map(AuditEvent::seq).distinct().count();
        assertEquals(events.size(), distinctSeqs, "sequence numbers must be unique and ordered");
        AuditEvent submitted = events.stream().filter(e -> e.type() == EventType.RUN_SUBMITTED)
                .findFirst().orElseThrow();
        assertEquals("audit-key", submitted.idempotencyKey());
        assertEquals(v.versionId(), submitted.definitionVersion());
        assertTrue(events.stream().anyMatch(e -> e.type() == EventType.RUN_SUCCEEDED));
        assertTrue(events.stream().anyMatch(e -> e.type() == EventType.RELEASE_PUBLISHED));
        log.info("audit-order pipeline=rel4 count={} flow={}", events.size(),
                events.stream().map(e -> e.type() + "[" + e.fromStatus() + "->" + e.toStatus() + "]")
                        .toList());
    }
}
