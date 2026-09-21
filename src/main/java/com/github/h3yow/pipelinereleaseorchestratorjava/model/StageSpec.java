package com.github.h3yow.pipelinereleaseorchestratorjava.model;

/** Immutable specification of a pipeline stage. */
public record StageSpec(String name, boolean retryable) {
    public StageSpec {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("stage name must not be blank");
        }
    }
}
