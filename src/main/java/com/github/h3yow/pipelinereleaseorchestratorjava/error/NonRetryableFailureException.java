package com.github.h3yow.pipelinereleaseorchestratorjava.error;
public class NonRetryableFailureException extends PipelineException {
    public NonRetryableFailureException(String message) { super(message); }
}
