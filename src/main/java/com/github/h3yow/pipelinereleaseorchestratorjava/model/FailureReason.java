package com.github.h3yow.pipelinereleaseorchestratorjava.model;

public enum FailureReason {
    NONE,
    STAGE_FAILED_RETRYABLE,
    STAGE_FAILED_FATAL,
    RETRIES_EXHAUSTED,
    CANCELLED,
    DEFINITION_WITHDRAWN,
    DEFINITION_INVALID
}
