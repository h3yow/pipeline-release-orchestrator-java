package com.github.h3yow.pipelinereleaseorchestratorjava.service.stage;

import com.github.h3yow.pipelinereleaseorchestratorjava.model.PipelineRun;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.StageSpec;

/** Default stage handler: every stage succeeds. */
public class AlwaysSucceedStageHandler implements StageHandler {

    @Override
    public StageResult execute(StageSpec stage, PipelineRun run, int attemptIndex) {
        return StageResult.ok();
    }
}
