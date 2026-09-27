package com.github.h3yow.pipelinereleaseorchestratorjava.model;

/**
 * Run state machine:
 * PENDING -> RUNNING -> (SUCCEEDED | FAILED | FAILED_NON_RETRYABLE | CANCELLED)
 * RUNNING/FAILED -> WAITING_RETRY -> RUNNING ...
 * RUNNING/WAITING_RETRY -> CANCELLING -> CANCELLED
 * any non-terminal --abort--> ABORTED
 * any non-terminal --bound definition version withdrawn--> DEFINITION_REVOKED
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
    /** Immediately aborted mid-flight; interrupt-driven, distinct from a graceful cancel. */
    ABORTED,
    /** Terminated because the bound definition version was withdrawn while in flight. */
    DEFINITION_REVOKED;

    public boolean isTerminal() {
        return this == SUCCEEDED || this == FAILED || this == FAILED_NON_RETRYABLE
                || this == CANCELLED || this == ABORTED || this == DEFINITION_REVOKED;
    }

    /** Whether a run in this state still consumes an in-flight slot. */
    public boolean occupiesSlot() {
        return !isTerminal();
    }
}
