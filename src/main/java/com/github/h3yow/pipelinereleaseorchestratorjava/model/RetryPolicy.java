package com.github.h3yow.pipelinereleaseorchestratorjava.model;

/** Configurable retry policy: maxAttempts includes the initial attempt; backoffMillis grows exponentially. */
public record RetryPolicy(int maxAttempts, long initialBackoffMillis, long maxBackoffMillis, double multiplier) {
}
