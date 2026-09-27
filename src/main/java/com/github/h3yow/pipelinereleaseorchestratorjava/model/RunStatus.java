package com.github.h3yow.pipelinereleaseorchestratorjava.model;

/**
 * Run state machine:
 * PENDING -> RUNNING -> (SUCCEEDED | FAILED | FAILED_NON_RETRYABLE | CANCELLED)
 * RUNNING/FAILED -> WAITING_RETRY -> RUNNING ...
 * RUNNING/WAITING_RETRY -> CANCELLING -> CANCELLED
 * any non-terminal -> ABORTED (immediate abort, interrupting the in-flight stage)
 * any non-terminal -> INVALIDATED (bound definition version was withdrawn mid-run)
 */
public enum RunStatus {
    PENDING,
    RUNNING,
    WAITING_RETRY,
    CANCELLING,
    SUCCEEDED,
    FAILED,
    FAILED_NON_RETRYABLE,
    CANCELLED,
    ABORTED,
    INVALIDATED;

    public boolean isTerminal() {
        return this == SUCCEEDED || this == FAILED || this == FAILED_NON_RETRYABLE
                || this == CANCELLED || this == ABORTED || this == INVALIDATED;
    }

    /** Whether a run in this state still consumes an in-flight slot. */
    public boolean occupiesSlot() {
        return !isTerminal();
    }
}
