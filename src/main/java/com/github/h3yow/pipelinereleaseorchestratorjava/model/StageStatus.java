package com.github.h3yow.pipelinereleaseorchestratorjava.model;

/** Per-stage status within a run. */
public enum StageStatus {
    PENDING,
    RUNNING,
    SUCCEEDED,
    FAILED,
    SKIPPED
}
