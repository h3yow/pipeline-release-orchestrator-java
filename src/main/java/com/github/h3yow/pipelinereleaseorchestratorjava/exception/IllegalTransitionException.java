package com.github.h3yow.pipelinereleaseorchestratorjava.exception;

import com.github.h3yow.pipelinereleaseorchestratorjava.model.RejectReason;

import static com.github.h3yow.pipelinereleaseorchestratorjava.model.RejectReason.ILLEGAL_TRANSITION;

/** Stage / promotion / state transition violates the state machine. */
public class IllegalTransitionException extends PipelineException {
    public IllegalTransitionException(String message) {
        super(ILLEGAL_TRANSITION, message);
    }

    public IllegalTransitionException(RejectReason reason, String message) {
        super(reason, message);
    }
}
