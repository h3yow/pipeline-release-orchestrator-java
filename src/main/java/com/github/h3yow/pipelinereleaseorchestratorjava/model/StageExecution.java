package com.github.h3yow.pipelinereleaseorchestratorjava.model;

import java.time.Instant;

/** Runtime state of a single stage within a run. */
public final class StageExecution {

    private String name;
    private int index;
    private StageStatus status;
    private int attempts;
    private Instant startedAt;
    private Instant finishedAt;
    private String lastError;
    private boolean lastErrorRetryable;

    public StageExecution() {
    }

    public StageExecution(String name, int index) {
        this.name = name;
        this.index = index;
        this.status = StageStatus.PENDING;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public StageStatus getStatus() {
        return status;
    }

    public void setStatus(StageStatus status) {
        this.status = status;
    }

    public int getAttempts() {
        return attempts;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(Instant startedAt) {
        this.startedAt = startedAt;
    }

    public Instant getFinishedAt() {
        return finishedAt;
    }

    public void setFinishedAt(Instant finishedAt) {
        this.finishedAt = finishedAt;
    }

    public String getLastError() {
        return lastError;
    }

    public void setLastError(String lastError) {
        this.lastError = lastError;
    }

    public boolean isLastErrorRetryable() {
        return lastErrorRetryable;
    }

    public void setLastErrorRetryable(boolean lastErrorRetryable) {
        this.lastErrorRetryable = lastErrorRetryable;
    }
}
