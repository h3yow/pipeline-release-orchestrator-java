package com.github.h3yow.pipelinereleaseorchestratorjava.tests;

import com.github.h3yow.pipelinereleaseorchestratorjava.config.OrchestratorProperties;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.PipelineDefinition;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.RetryPolicy;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.StageSpec;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.PipelineLockManager;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.impl.AuditSupport;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.impl.DefaultDefinitionService;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.impl.DefaultReleaseService;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.impl.DefaultRunService;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.stage.StageHandler;
import com.github.h3yow.pipelinereleaseorchestratorjava.store.LocalStateStore;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

/** Manual (non-Spring) wiring for fast unit tests, persistence disabled by default. */
final class TestEnv {

    final OrchestratorProperties properties;
    final LocalStateStore store;
    final PipelineLockManager locks;
    final AuditSupport audit;
    final DefaultDefinitionService definitions;
    final DefaultRunService runs;
    final DefaultReleaseService releases;

    private TestEnv(OrchestratorProperties properties, StageHandler handler) {
        this.properties = properties;
        this.store = new LocalStateStore(properties);
        this.locks = new PipelineLockManager();
        this.audit = new AuditSupport(store);
        this.definitions = new DefaultDefinitionService(store, locks, audit);
        this.runs = new DefaultRunService(store, locks, definitions, audit, handler, properties);
        this.definitions.setWithdrawalListener(this.runs);
        this.releases = new DefaultReleaseService(store, locks, definitions, audit);    }

    static TestEnv create(StageHandler handler) {
        return new TestEnv(props("REJECT"), handler);
    }

    static TestEnv create(StageHandler handler, String overflowPolicy) {
        return new TestEnv(props(overflowPolicy), handler);
    }

    static OrchestratorProperties props(String overflowPolicy) {
        OrchestratorProperties p = new OrchestratorProperties();
        p.setPersistenceEnabled(false);
        p.setStorageDir("target/test-data-disabled");
        p.setMaxInFlightPerPipeline(2);
        p.setMaxQueuedPerPipeline(2);
        p.setOverflowPolicy(overflowPolicy);
        return p;
    }

    static PipelineDefinition definition(String name, int maxRetries, Duration backoff,
                                         List<String> envs, String... stageNames) {
        return new PipelineDefinition(name, stages(stageNames), envs, retry(maxRetries, backoff));
    }

    static List<StageSpec> stages(String... names) {
        return Arrays.stream(names).map(n -> new StageSpec(n, true)).toList();
    }

    static RetryPolicy retry(int maxRetries, Duration backoff) {
        return new RetryPolicy(maxRetries, backoff, 2.0, Duration.ofSeconds(30));
    }
}
