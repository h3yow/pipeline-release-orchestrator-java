package com.github.h3yow.pipelinereleaseorchestratorjava.model;

/**
 * Run state machine:
 * PENDING -> RUNNING -> (SUCCEEDED | FAILED | FAILED_NON_RETRYABLE | CANCELLED)
 * RUNNING/FAILED -> WAITING_RETRY -> RUNNING ...
 * RUNNING/WAITING_RETRY -> CANCELLING -> CANCELLED
 */
public enum RunStatus {
    PENDING,
    RUNNING,
    WAITING_RETRY,
    CANCELLING,
    SUCCEEDED,
    FAILED,
    FAILED_NON_RETRYABLE,
    CANCELLED;

    public boolean isTerminal() {
        return this == SUCCEEDED || this == FAILED || this == FAILED_NON_RETRYABLE || this == CANCELLED;
    }

    /** Whether a run in this state still consumes an in-flight slot. */
    public boolean occupiesSlot() {
        return !isTerminal();
    }
}
