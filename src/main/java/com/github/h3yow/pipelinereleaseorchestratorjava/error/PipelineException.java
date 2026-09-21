package com.github.h3yow.pipelinereleaseorchestratorjava.error;

/** Base class for all distinguishable business errors. */
public class PipelineException extends RuntimeException {
    public PipelineException(String message) { super(message); }
}
