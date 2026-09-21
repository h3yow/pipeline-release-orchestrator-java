package com.github.h3yow.pipelinereleaseorchestratorjava.model;

import java.util.List;

/** Immutable definition of a single stage. {@code retryable} determines whether a failure may be retried. */
public record StageSpec(String name, boolean retryable, List<String> environments) {
    public StageSpec {
        environments = environments == null ? List.of() : List.copyOf(environments);
    }
}
