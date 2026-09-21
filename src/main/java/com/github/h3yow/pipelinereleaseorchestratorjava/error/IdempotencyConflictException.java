package com.github.h3yow.pipelinereleaseorchestratorjava.error;
public class IdempotencyConflictException extends PipelineException {
    public IdempotencyConflictException(String message) { super(message); }
}
