package com.github.h3yow.pipelinereleaseorchestratorjava.tests;

import com.github.h3yow.pipelinereleaseorchestratorjava.config.OrchestratorProperties;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.DefinitionVersion;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.PipelineRun;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.RunStatus;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.PipelineLockManager;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.impl.AuditSupport;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.impl.DefaultDefinitionService;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.impl.DefaultRunService;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.stage.AlwaysSucceedStageHandler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static com.github.h3yow.pipelinereleaseorchestratorjava.tests.TestEnv.definition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PersistenceTests {

    private static final Logger log = LoggerFactory.getLogger(PersistenceTests.class);

    @Test
    void snapshotAndAuditLogArePersistedAndReloadable(@TempDir Path dir) throws Exception {
        OrchestratorProperties props = TestEnv.props("REJECT");
        props.setPersistenceEnabled(true);
        props.setStorageDir(dir.toString());

        String runId;
        long auditCount;
        try (var ctx = new Env(props)) {
            DefinitionVersion v = ctx.definitions.createVersion("svc",
                    definition("svc", 1, Duration.ofMillis(1), List.of("dev"), "build", "test"));
            PipelineRun run = ctx.runs.submit("svc", v.getVersion(), "persist-1",
                    Map.of("commit", "abc"));
            runId = run.getRunId();
            PipelineRun done = ctx.runs.advance(runId);
            auditCount = ctx.store.allAuditEvents().size();
            assertEquals(RunStatus.SUCCEEDED, done.getStatus());
        }

        assertTrue(Files.exists(dir.resolve("state.json")), "snapshot file expected");
        Path auditFile = dir.resolve("audit.jsonl");
        assertTrue(Files.exists(auditFile), "audit log file expected");
        long auditLines;
        try (var lines = Files.lines(auditFile)) {
            auditLines = lines.filter(l -> !l.isBlank()).count();
        }
        log.info("[PERSIST] storageDir={} state.json exists, audit lines={}", dir, auditLines);
        assertEquals(auditCount, auditLines);

        // Reload into fresh in-process state: definitions, runs and audit trail survive.
        try (var reloaded = new Env(props)) {
            assertEquals(RunStatus.SUCCEEDED, reloaded.runs.get(runId).getStatus());
            assertEquals(2, reloaded.definitions.latest("svc").getDefinition().getStages().size());
            assertEquals(auditCount, reloaded.store.allAuditEvents().size());
            log.info("[PERSIST] reloaded run {} status={} audit events={}",
                    runId, reloaded.runs.get(runId).getStatus(),
                    reloaded.store.allAuditEvents().size());
        }
    }

    /** Tiny closeable wiring around the manual services. */
    private static final class Env implements AutoCloseable {
        final com.github.h3yow.pipelinereleaseorchestratorjava.store.LocalStateStore store;
        final PipelineLockManager locks;
        final AuditSupport audit;
        final DefaultDefinitionService definitions;
        final DefaultRunService runs;

        Env(OrchestratorProperties props) {
            this.store = new com.github.h3yow.pipelinereleaseorchestratorjava.store.LocalStateStore(props);
            this.locks = new PipelineLockManager();
            this.audit = new AuditSupport(store);
            this.definitions = new DefaultDefinitionService(store, locks, audit);
            this.runs = new DefaultRunService(store, locks, definitions, audit,
                    new AlwaysSucceedStageHandler(), props);
        }

        @Override
        public void close() {
        }
    }
}
