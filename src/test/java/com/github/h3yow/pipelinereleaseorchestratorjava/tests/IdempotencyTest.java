package com.github.h3yow.pipelinereleaseorchestratorjava.tests;

import com.github.h3yow.pipelinereleaseorchestratorjava.error.IdempotencyConflictException;
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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class IdempotencyTest extends HarnessCleanup {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyTest.class);

    @TempDir
    Path tempDir;

    @Test
    void sameKeyAndPayloadCreatesOneRunWithConsistentFinalResult() throws Exception {
        TestHarness harness = new TestHarness(tempDir);
        DefinitionVersion v = harness.define("idem", List.of("dev"),
                List.of(new StageSpec("build", true, List.of("dev"))));

        RunRecord first = harness.orchestrator.submitRun("idem", v.versionId(), "key-1", "{\"x\":1}");
        RunRecord second = harness.orchestrator.submitRun("idem", v.versionId(), "key-1", "{\"x\":1}");
        RunRecord third = harness.orchestrator.submitRun("idem", v.versionId(), "key-1", "{\"x\":1}");
        log.info("idempotent-submit key=key-1 version={} firstRun={} secondRun={} thirdRun={}",
                v.versionId(), first.runId(), second.runId(), third.runId());

        assertEquals(first.runId(), second.runId());
        assertEquals(first.runId(), third.runId());

        RunRecord done1 = harness.awaitTerminal(first.runId());
        RunRecord done2 = harness.awaitTerminal(second.runId());
        assertEquals(RunStatus.SUCCEEDED, done1.status());
        assertEquals(done1.status(), done2.status());
        assertEquals(done1.runId(), done2.runId());
        log.info("idempotent-final key=key-1 version={} run={} status={}", v.versionId(),
                done1.runId(), done1.status());
    }

    @Test
    void sameKeyWithDifferentPayloadIsRejectedNotSilentlyOverwritten() throws Exception {
        TestHarness harness = new TestHarness(tempDir);
        DefinitionVersion v = harness.define("idem2", List.of("dev"),
                List.of(new StageSpec("build", true, List.of("dev"))));

        RunRecord original = harness.orchestrator.submitRun("idem2", v.versionId(), "key-9",
                "{\"x\":1}");
        IdempotencyConflictException ex = assertThrows(IdempotencyConflictException.class,
                () -> harness.orchestrator.submitRun("idem2", v.versionId(), "key-9",
                        "{\"x\":2}"));
        log.info("idempotency-conflict key=key-9 version={} existingRun={} reason={}", v.versionId(),
                original.runId(), ex.getMessage());

        RunRecord untouched = harness.orchestrator.getRun(original.runId());
        assertEquals(original.runId(), untouched.runId());
        assertEquals("{\"x\":1}", payloadOf(untouched), "existing run must not be overwritten");
    }

    @Test
    void differentKeysProduceIndependentRuns() {
        TestHarness harness = new TestHarness(tempDir);
        DefinitionVersion v = harness.define("idem3", List.of("dev"),
                List.of(new StageSpec("build", true, List.of("dev"))));
        RunRecord a = harness.orchestrator.submitRun("idem3", v.versionId(), "ka", "p");
        RunRecord b = harness.orchestrator.submitRun("idem3", v.versionId(), "kb", "p");
        log.info("distinct-keys keys=ka,kb version={} runs={},{}", v.versionId(), a.runId(), b.runId());
        assertNotEquals(a.runId(), b.runId());
    }

    private static String payloadOf(RunRecord run) {
        // payloadHash is sha256 hex of the original payload; recompute for verification
        return "{\"x\":1}".equals("{\"x\":1}") && run.payloadHash().equals(
                com.github.h3yow.pipelinereleaseorchestratorjava.service.Idempotency
                        .sha256("{\"x\":1}")) ? "{\"x\":1}" : "?";
    }
}
