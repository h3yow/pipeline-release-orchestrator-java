package com.github.h3yow.pipelinereleaseorchestratorjava.tests;

import com.github.h3yow.pipelinereleaseorchestratorjava.error.DefinitionNotFoundException;
import com.github.h3yow.pipelinereleaseorchestratorjava.error.DefinitionWithdrawnException;
import com.github.h3yow.pipelinereleaseorchestratorjava.error.InvalidDefinitionException;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.DefinitionStatus;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.DefinitionVersion;
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

class DefinitionVersioningTest extends HarnessCleanup {

    private static final Logger log = LoggerFactory.getLogger(DefinitionVersioningTest.class);

    private final List<String> envs = List.of("dev", "prod");
    private final List<StageSpec> stages = List.of(
            new StageSpec("build", true, List.of("dev", "prod")),
            new StageSpec("deploy", true, List.of("dev", "prod")));

    @TempDir
    Path tempDir;

    @Test
    void identicalContentProducesSameImmutableVersionAndChangedContentANewOne() {
        TestHarness harness = new TestHarness(tempDir);
        DefinitionVersion v1 = harness.define("p1", envs, stages);
        DefinitionVersion v1Again = harness.define("p1", envs, stages);
        log.info("idempotent-definition version={} status={}", v1.versionId(), v1.status());
        assertEquals(v1.versionId(), v1Again.versionId(), "same content must map to the same version");

        List<StageSpec> changed = List.of(
                new StageSpec("build", true, List.of("dev", "prod")),
                new StageSpec("deploy", false, List.of("dev", "prod")));
        DefinitionVersion v2 = harness.define("p1", envs, changed);
        assertNotEquals(v1.versionId(), v2.versionId(), "changed content must produce a new version");
        log.info("new-definition oldVersion={} newVersion={}", v1.versionId(), v2.versionId());
        assertEquals(DefinitionStatus.ACTIVE, v1.status(), "older versions stay immutable and active");
    }

    @Test
    void withdrawnVersionCannotBeExecutedWithDistinguishableReason() {
        TestHarness harness = new TestHarness(tempDir);
        DefinitionVersion v = harness.define("p2", envs, stages);
        harness.definitionService.withdraw("p2", v.versionId());
        assertEquals(DefinitionStatus.WITHDRAWN,
                harness.definitionService.getVersion("p2", v.versionId()).status());
        log.info("withdraw version={} status=WITHDRAWN", v.versionId());

        DefinitionWithdrawnException ex = assertThrows(DefinitionWithdrawnException.class,
                () -> harness.orchestrator.submitRun("p2", v.versionId(), "key-w", "{}"));
        log.info("submit-on-withdrawn version={} rejected reason={}", v.versionId(), ex.getMessage());
    }

    @Test
    void unknownVersionAndIllegalDefinitionsAreRejected() {
        TestHarness harness = new TestHarness(tempDir);
        assertThrows(DefinitionNotFoundException.class,
                () -> harness.orchestrator.submitRun("p3", "v-nope", "key-x", "{}"));

        List<StageSpec> badStages = List.of(
                new StageSpec("build", true, List.of("nonexistent-env")));
        assertThrows(InvalidDefinitionException.class,
                () -> harness.define("p3", envs, badStages));
        assertThrows(InvalidDefinitionException.class,
                () -> harness.define("p3", List.of(), stages));
        log.info("invalid-definition checks passed");
    }
}
