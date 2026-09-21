package com.github.h3yow.pipelinereleaseorchestratorjava.model;

public enum RunStatus {
    QUEUED,
    WAITING_PROMOTION,
    RUNNING,
    SUCCEEDED,
    FAILED_RETRYABLE,
    FAILED_FATAL,
    CANCELLED
}
