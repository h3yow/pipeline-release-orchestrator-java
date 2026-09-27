package com.github.h3yow.pipelinereleaseorchestratorjava.model;

/** Per-stage status within a run. */
public enum StageStatus {
    PENDING,
    RUNNING,
    SUCCEEDED,
    FAILED,
    /** Broken off mid-execution by an abort / definition revocation; never a success trace. */
    INTERRUPTED,
    SKIPPED
}
