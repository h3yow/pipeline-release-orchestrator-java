package com.github.h3yow.pipelinereleaseorchestratorjava.web;

import com.github.h3yow.pipelinereleaseorchestratorjava.api.Dtos.ErrorResponse;
import com.github.h3yow.pipelinereleaseorchestratorjava.error.CapacityRejectedException;
import com.github.h3yow.pipelinereleaseorchestratorjava.error.DefinitionNotFoundException;
import com.github.h3yow.pipelinereleaseorchestratorjava.error.DefinitionWithdrawnException;
import com.github.h3yow.pipelinereleaseorchestratorjava.error.IdempotencyConflictException;
import com.github.h3yow.pipelinereleaseorchestratorjava.error.IllegalRunStateException;
import com.github.h3yow.pipelinereleaseorchestratorjava.error.InvalidDefinitionException;
import com.github.h3yow.pipelinereleaseorchestratorjava.error.RollbackNotAllowedException;
import com.github.h3yow.pipelinereleaseorchestratorjava.error.RunNotFoundException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** Maps each business error to a distinct status code so failure reasons stay distinguishable. */
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler({InvalidDefinitionException.class, IdempotencyConflictException.class})
    public ResponseEntity<ErrorResponse> badRequest(RuntimeException e) {
        return build(HttpStatus.BAD_REQUEST, e);
    }

    @ExceptionHandler({DefinitionNotFoundException.class, RunNotFoundException.class})
    public ResponseEntity<ErrorResponse> notFound(RuntimeException e) {
        return build(HttpStatus.NOT_FOUND, e);
    }

    @ExceptionHandler({DefinitionWithdrawnException.class, IllegalRunStateException.class,
            RollbackNotAllowedException.class})
    public ResponseEntity<ErrorResponse> conflict(RuntimeException e) {
        return build(HttpStatus.CONFLICT, e);
    }

    @ExceptionHandler(CapacityRejectedException.class)
    public ResponseEntity<ErrorResponse> tooManyRequests(CapacityRejectedException e) {
        return build(HttpStatus.TOO_MANY_REQUESTS, e);
    }

    private ResponseEntity<ErrorResponse> build(HttpStatus status, RuntimeException e) {
        return ResponseEntity.status(status)
                .body(new ErrorResponse(e.getClass().getSimpleName(), e.getMessage()));
    }
}
