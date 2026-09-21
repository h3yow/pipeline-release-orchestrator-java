package com.github.h3yow.pipelinereleaseorchestratorjava.exception;

import com.github.h3yow.pipelinereleaseorchestratorjava.model.RejectReason;

/** Same idempotency key reused with a different payload. */
public class IdempotencyConflictException extends PipelineException {

    private final String existingRunId;

    public IdempotencyConflictException(String message, String existingRunId) {
        super(RejectReason.IDEMPOTENT_KEY_PAYLOAD_MISMATCH, message);
        this.existingRunId = existingRunId;
    }

    public String getExistingRunId() {
        return existingRunId;
    }
}
