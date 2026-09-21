package com.github.h3yow.pipelinereleaseorchestratorjava.model;

import java.time.Instant;

/** Execution record of one stage attempt. */
public record StageExecution(String stage, String environment, int attempt, boolean success,
                             boolean fatal, String message, Instant at) {
}
