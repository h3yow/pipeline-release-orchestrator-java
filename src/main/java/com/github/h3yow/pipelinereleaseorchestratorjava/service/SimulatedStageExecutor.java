package com.github.h3yow.pipelinereleaseorchestratorjava.service;

import com.github.h3yow.pipelinereleaseorchestratorjava.model.StageSpec;

/**
 * Default in-process stage executor: every stage succeeds immediately.
 * Tests inject their own {@link StageExecutor} to script retryable/fatal failures.
 */
public class SimulatedStageExecutor implements StageExecutor {

    public SimulatedStageExecutor() {
    }

    @Override
    public StageResult execute(String runId, String pipelineId, String versionId, StageSpec stage,
                               String environment, int attempt) {
        return StageResult.ok();
    }
}
