package com.github.h3yow.pipelinereleaseorchestratorjava.tests;

import com.github.h3yow.pipelinereleaseorchestratorjava.exception.DefinitionRejectedException;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.DefinitionStatus;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.DefinitionVersion;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.RejectReason;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.stage.AlwaysSucceedStageHandler;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

import static com.github.h3yow.pipelinereleaseorchestratorjava.tests.TestEnv.definition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefinitionVersioningTests {

    private static final Logger log = LoggerFactory.getLogger(DefinitionVersioningTests.class);

    @Test
    void everyChangeCreatesANewImmutableVersion() {
        TestEnv env = TestEnv.create(new AlwaysSucceedStageHandler());
        DefinitionVersion v1 = env.definitions.createVersion("svc",
                definition("svc", 1, Duration.ofMillis(10), List.of("dev"), "build", "test"));
        DefinitionVersion v2 = env.definitions.createVersion("svc",
                definition("svc", 2, Duration.ofMillis(20), List.of("dev"), "build", "test", "deploy"));

        log.info("[DEF] versions: v{} hash={} | v{} hash={}",
                v1.getVersion(), v1.getContentHash(), v2.getVersion(), v2.getContentHash());

        assertEquals(1, v1.getVersion());
        assertEquals(2, v2.getVersion());
        assertEquals(DefinitionStatus.ACTIVE, v1.getStatus());
        assertNotEquals(v1.getContentHash(), v2.getContentHash());
        // Immutability: old snapshot still exposes its own stage count.
        assertEquals(2, v1.getDefinition().getStages().size());
        assertEquals(3, v2.getDefinition().getStages().size());
    }

    @Test
    void withdrawnVersionIsNotExecutableWithDistinctReason() {
        TestEnv env = TestEnv.create(new AlwaysSucceedStageHandler());
        DefinitionVersion v1 = env.definitions.createVersion("svc",
                definition("svc", 0, Duration.ZERO, List.of("dev"), "build"));
        env.definitions.withdraw("svc", v1.getVersion(), "bad version");

        DefinitionRejectedException ex = assertThrows(DefinitionRejectedException.class,
                () -> env.runs.submit("svc", v1.getVersion(), "k-1", java.util.Map.of("x", "1")));
        log.info("[DEF] withdrawn submit rejected reason={} message={}", ex.getReason(), ex.getMessage());
        assertEquals(RejectReason.DEFINITION_WITHDRAWN, ex.getReason());
    }

    @Test
    void missingAndInvalidVersionsHaveDistinctReasons() {
        TestEnv env = TestEnv.create(new AlwaysSucceedStageHandler());
        DefinitionRejectedException missing = assertThrows(DefinitionRejectedException.class,
                () -> env.runs.submit("ghost", 99, "k", java.util.Map.of()));
        assertEquals(RejectReason.DEFINITION_NOT_FOUND, missing.getReason());

        DefinitionRejectedException invalid = assertThrows(DefinitionRejectedException.class,
                () -> env.definitions.createVersion("bad",
                        new com.github.h3yow.pipelinereleaseorchestratorjava.model.PipelineDefinition(
                                "bad", List.of(), List.of("dev"),
                                TestEnv.retry(0, Duration.ZERO))));
        log.info("[DEF] invalid definition rejected reason={}", invalid.getReason());
        assertEquals(RejectReason.DEFINITION_INVALID, invalid.getReason());
        assertTrue(invalid.getMessage().contains("at least one stage"));
    }
}
