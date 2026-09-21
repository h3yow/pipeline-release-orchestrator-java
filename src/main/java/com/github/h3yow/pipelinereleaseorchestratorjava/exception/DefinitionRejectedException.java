package com.github.h3yow.pipelinereleaseorchestratorjava.exception;

import com.github.h3yow.pipelinereleaseorchestratorjava.model.RejectReason;

/** A definition version cannot be executed (withdrawn, invalid or missing). */
public class DefinitionRejectedException extends PipelineException {
    public DefinitionRejectedException(RejectReason reason, String message) {
        super(reason, message);
    }
}
