package com.github.h3yow.pipelinereleaseorchestratorjava.service;

import com.github.h3yow.pipelinereleaseorchestratorjava.model.RunRecord;

/**
 * Thin delegate that drives one run to its next stopping point synchronously.
 * The orchestrator normally schedules work on its own worker pool; this class is also convenient
 * for deterministic tests that want to execute on the calling thread.
 */
public class RunWorker {

    private final PipelineOrchestrator orchestrator;

    public RunWorker(PipelineOrchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }

    public void executeRun(RunRecord run) {
        orchestrator.executeQueuedRun(run);
    }
}
