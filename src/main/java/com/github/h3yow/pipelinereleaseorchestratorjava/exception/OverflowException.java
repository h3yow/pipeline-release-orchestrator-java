package com.github.h3yow.pipelinereleaseorchestratorjava.exception;

import com.github.h3yow.pipelinereleaseorchestratorjava.model.RejectReason;

/** Concurrency / backlog cap exceeded. */
public class OverflowException extends PipelineException {
    public OverflowException(RejectReason reason, String message) {
        super(reason, message);
    }
}
