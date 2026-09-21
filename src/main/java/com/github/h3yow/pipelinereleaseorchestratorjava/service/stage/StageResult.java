package com.github.h3yow.pipelinereleaseorchestratorjava.service.stage;

/** Outcome of executing one stage once. */
public record StageResult(boolean success, boolean retryable, String errorMessage) {

    public static StageResult ok() {
        return new StageResult(true, false, null);
    }

    public static StageResult fail(boolean retryable, String errorMessage) {
        return new StageResult(false, retryable, errorMessage);
    }
}
