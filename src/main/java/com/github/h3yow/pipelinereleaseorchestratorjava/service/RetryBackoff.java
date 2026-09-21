package com.github.h3yow.pipelinereleaseorchestratorjava.service;

import com.github.h3yow.pipelinereleaseorchestratorjava.model.RetryPolicy;

/**
 * Deterministic exponential backoff: {@code min(initialBackoff * multiplier^(attempt-1), maxBackoff)}.
 * No jitter, so retry timing is reproducible in tests. {@code attempt} is 1-based for the
 * already-failed attempt; the returned delay is the wait before the next attempt.
 */
public class RetryBackoff {

    public RetryBackoff() {
    }

    public long delayMillis(RetryPolicy policy, int attempt) {
        if (policy == null || attempt < 1) {
            return 0L;
        }
        double delay = policy.initialBackoffMillis();
        for (int i = 1; i < attempt; i++) {
            delay *= policy.multiplier();
            if (delay >= policy.maxBackoffMillis()) {
                return policy.maxBackoffMillis();
            }
        }
        return Math.min(policy.maxBackoffMillis(), Math.max(0L, (long) delay));
    }
}
