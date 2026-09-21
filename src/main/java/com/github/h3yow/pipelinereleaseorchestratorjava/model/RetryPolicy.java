package com.github.h3yow.pipelinereleaseorchestratorjava.model;

import java.time.Duration;

/** Configurable retry policy: max attempts (excluding the first try) and backoff. */
public final class RetryPolicy {

    private int maxAttempts;
    private Duration initialBackoff;
    private double backoffMultiplier;
    private Duration maxBackoff;

    public RetryPolicy() {
    }

    public RetryPolicy(int maxAttempts, Duration initialBackoff, double backoffMultiplier, Duration maxBackoff) {
        this.maxAttempts = maxAttempts;
        this.initialBackoff = initialBackoff;
        this.backoffMultiplier = backoffMultiplier;
        this.maxBackoff = maxBackoff;
    }

    public static RetryPolicy noRetry() {
        return new RetryPolicy(0, Duration.ZERO, 1.0, Duration.ZERO);
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public Duration getInitialBackoff() {
        return initialBackoff;
    }

    public void setInitialBackoff(Duration initialBackoff) {
        this.initialBackoff = initialBackoff;
    }

    public double getBackoffMultiplier() {
        return backoffMultiplier;
    }

    public void setBackoffMultiplier(double backoffMultiplier) {
        this.backoffMultiplier = backoffMultiplier;
    }

    public Duration getMaxBackoff() {
        return maxBackoff;
    }

    public void setMaxBackoff(Duration maxBackoff) {
        this.maxBackoff = maxBackoff;
    }

    /**
     * Backoff before the given retry attempt. {@code retryAttempt} is 1-based for retries
     * (1 = backoff before the second overall execution).
     */
    public Duration backoffForAttempt(int retryAttempt) {
        if (retryAttempt <= 0 || initialBackoff == null || initialBackoff.isZero()) {
            return Duration.ZERO;
        }
        long millis = (long) (initialBackoff.toMillis()
                * Math.pow(backoffMultiplier <= 0 ? 1.0 : backoffMultiplier, retryAttempt - 1));
        long cap = maxBackoff == null ? Long.MAX_VALUE : maxBackoff.toMillis();
        return Duration.ofMillis(Math.min(Math.max(millis, 0), cap));
    }
}
