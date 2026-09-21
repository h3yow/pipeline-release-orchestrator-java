package com.github.h3yow.pipelinereleaseorchestratorjava.service;

import com.github.h3yow.pipelinereleaseorchestratorjava.model.StageSpec;

/**
 * Simulated stage execution. Implementations return a StageResult; throwing or returning
 * fatal=true makes the failure non-retryable.
 */
public interface StageExecutor {
    StageResult execute(String runId, String pipelineId, String versionId, StageSpec stage,
                        String environment, int attempt);

    record StageResult(boolean success, boolean fatal, String message) {
        public static StageResult ok() { return new StageResult(true, false, "ok"); }
        public static StageResult retryableFailure(String message) { return new StageResult(false, false, message); }
        public static StageResult fatalFailure(String message) { return new StageResult(false, true, message); }
    }
}
