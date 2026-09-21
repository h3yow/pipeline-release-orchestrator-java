package com.github.h3yow.pipelinereleaseorchestratorjava.service.stage;

import com.github.h3yow.pipelinereleaseorchestratorjava.model.PipelineRun;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.StageSpec;

/** Pluggable per-stage execution logic. */
@FunctionalInterface
public interface StageHandler {

    StageResult execute(StageSpec stage, PipelineRun run, int attemptIndex) throws Exception;
}
