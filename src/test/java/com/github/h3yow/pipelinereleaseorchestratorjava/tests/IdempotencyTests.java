package com.github.h3yow.pipelinereleaseorchestratorjava.tests;

import com.github.h3yow.pipelinereleaseorchestratorjava.exception.IdempotencyConflictException;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.DefinitionVersion;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.PipelineRun;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.RunStatus;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.stage.AlwaysSucceedStageHandler;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static com.github.h3yow.pipelinereleaseorchestratorjava.tests.TestEnv.definition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class IdempotencyTests {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyTests.class);

    @Test
    void duplicateSubmitWithSameKeyAndPayloadProducesOneRun() {
        TestEnv env = TestEnv.create(new AlwaysSucceedStageHandler());
        DefinitionVersion v = env.definitions.createVersion("svc",
                definition("svc", 0, Duration.ZERO, List.of("dev"), "build", "test"));

        PipelineRun first = env.runs.submit("svc", v.getVersion(), "idem-1", Map.of("branch", "main"));
        PipelineRun second = env.runs.submit("svc", v.getVersion(), "idem-1", Map.of("branch", "main"));

        log.info("[IDEM] key=idem-1 firstRunId={} secondRunId={} status={}",
                first.getRunId(), second.getRunId(), second.getStatus());

        assertSame(first, second);
        assertEquals(1, env.store.listRuns("svc").size());
        // Completing via the de-duplicated handle yields the same final result.
        PipelineRun finished = env.runs.advance(second.getRunId());
        assertEquals(RunStatus.SUCCEEDED, finished.getStatus());
        assertEquals(RunStatus.SUCCEEDED, env.runs.submit("svc", v.getVersion(), "idem-1",
                Map.of("branch", "main")).getStatus());
    }

    @Test
    void sameKeyWithDifferentPayloadIsRejectedAndNeverOverwritesExistingRun() {
        TestEnv env = TestEnv.create(new AlwaysSucceedStageHandler());
        DefinitionVersion v = env.definitions.createVersion("svc",
                definition("svc", 0, Duration.ZERO, List.of("dev"), "build"));

        PipelineRun first = env.runs.submit("svc", v.getVersion(), "idem-2",
                Map.of("commit", "aaa"));

        IdempotencyConflictException ex = assertThrows(IdempotencyConflictException.class,
                () -> env.runs.submit("svc", v.getVersion(), "idem-2", Map.of("commit", "bbb")));
        log.info("[IDEM] key=idem-2 conflict existingRunId={} reason={}",
                ex.getExistingRunId(), ex.getReason());

        assertEquals(first.getRunId(), ex.getExistingRunId());
        // Original run survives untouched and is still usable.
        PipelineRun untouched = env.runs.get(first.getRunId());
        assertEquals(first.getPayloadHash(), untouched.getPayloadHash());
        assertEquals(RunStatus.SUCCEEDED, env.runs.advance(first.getRunId()).getStatus());
        assertEquals(1, env.store.listRuns("svc").size());
    }
}
