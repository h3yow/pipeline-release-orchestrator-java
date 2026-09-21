package com.github.h3yow.pipelinereleaseorchestratorjava.exception;

import com.github.h3yow.pipelinereleaseorchestratorjava.model.RejectReason;

/** Base class for all distinguishable pipeline rejections / failures. */
public class PipelineException extends RuntimeException {

    private final RejectReason reason;

    public PipelineException(RejectReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public PipelineException(RejectReason reason, String message, Throwable cause) {
        super(message, cause);
        this.reason = reason;
    }

    public RejectReason getReason() {
        return reason;
    }
}
