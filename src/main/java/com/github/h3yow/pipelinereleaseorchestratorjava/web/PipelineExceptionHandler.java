package com.github.h3yow.pipelinereleaseorchestratorjava.web;

import com.github.h3yow.pipelinereleaseorchestratorjava.exception.IdempotencyConflictException;
import com.github.h3yow.pipelinereleaseorchestratorjava.exception.OverflowException;
import com.github.h3yow.pipelinereleaseorchestratorjava.exception.PipelineException;
import com.github.h3yow.pipelinereleaseorchestratorjava.web.dto.ErrorResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** Maps distinguishable domain failures onto stable HTTP status + reason codes. */
@RestControllerAdvice
public class PipelineExceptionHandler {

    @ExceptionHandler(IdempotencyConflictException.class)
    public ResponseEntity<ErrorResponse> conflict(PipelineException ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(new ErrorResponse(ex.getReason(), ex.getMessage()));
    }

    @ExceptionHandler(OverflowException.class)
    public ResponseEntity<ErrorResponse> overflow(PipelineException ex) {
        // Caller must back off / queue; distinguishable from a plain conflict.
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .body(new ErrorResponse(ex.getReason(), ex.getMessage()));
    }

    @ExceptionHandler(PipelineException.class)
    public ResponseEntity<ErrorResponse> unprocessable(PipelineException ex) {
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                .body(new ErrorResponse(ex.getReason(), ex.getMessage()));
    }
}
