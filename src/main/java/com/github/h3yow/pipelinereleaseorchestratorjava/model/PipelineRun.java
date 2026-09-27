package com.github.h3yow.pipelinereleaseorchestratorjava.model;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/** A run bound to an immutable definition version. */
public final class PipelineRun {

    private String runId;
    private String pipelineName;
    private int definitionVersion;
    private String idempotencyKey;
    private String payloadHash;
    private RunStatus status;
    private List<StageExecution> stages = new ArrayList<>();
    private int currentStageIndex;
    private int retryAttempts;
    private Instant retryNotBefore;
    private boolean cancelRequested;
    /**
     * Lock-free stop signal for immediate abort: set before the pipeline lock is
     * acquired so a drive loop executing a long stage on another thread observes it
     * (and its executing thread is interrupted) as early as possible.
     */
    private volatile boolean abortRequested;
    /** Reason supplied by the abort trigger; read by the drive loop when converging. */
    private volatile String abortReason;
    private boolean queued;
    private Instant createdAt;
    private Instant updatedAt;
    private Instant finishedAt;
    private String failureReason;
    private RejectReason failureCode;
    private List<String> promotedEnvironments = new ArrayList<>();

    public PipelineRun() {
    }

    public String getRunId() {
        return runId;
    }

    public void setRunId(String runId) {
        this.runId = runId;
    }

    public String getPipelineName() {
        return pipelineName;
    }

    public void setPipelineName(String pipelineName) {
        this.pipelineName = pipelineName;
    }

    public int getDefinitionVersion() {
        return definitionVersion;
    }

    public void setDefinitionVersion(int definitionVersion) {
        this.definitionVersion = definitionVersion;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public void setIdempotencyKey(String idempotencyKey) {
        this.idempotencyKey = idempotencyKey;
    }

    public String getPayloadHash() {
        return payloadHash;
    }

    public void setPayloadHash(String payloadHash) {
        this.payloadHash = payloadHash;
    }

    public RunStatus getStatus() {
        return status;
    }

    public void setStatus(RunStatus status) {
        this.status = status;
    }

    public List<StageExecution> getStages() {
        return stages;
    }

    public void setStages(List<StageExecution> stages) {
        this.stages = stages;
    }

    public int getCurrentStageIndex() {
        return currentStageIndex;
    }

    public void setCurrentStageIndex(int currentStageIndex) {
        this.currentStageIndex = currentStageIndex;
    }

    public int getRetryAttempts() {
        return retryAttempts;
    }

    public void setRetryAttempts(int retryAttempts) {
        this.retryAttempts = retryAttempts;
    }

    public Instant getRetryNotBefore() {
        return retryNotBefore;
    }

    public void setRetryNotBefore(Instant retryNotBefore) {
        this.retryNotBefore = retryNotBefore;
    }

    public boolean isCancelRequested() {
        return cancelRequested;
    }

    public void setCancelRequested(boolean cancelRequested) {
        this.cancelRequested = cancelRequested;
    }

    public boolean isAbortRequested() {
        return abortRequested;
    }

    public void setAbortRequested(boolean abortRequested) {
        this.abortRequested = abortRequested;
    }

    public String getAbortReason() {
        return abortReason;
    }

    public void setAbortReason(String abortReason) {
        this.abortReason = abortReason;
    }

    /** True when the run is PENDING only because of the in-flight cap (QUEUE policy). */
    public boolean isQueued() {
        return queued;
    }

    public void setQueued(boolean queued) {
        this.queued = queued;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }

    public Instant getFinishedAt() {
        return finishedAt;
    }

    public void setFinishedAt(Instant finishedAt) {
        this.finishedAt = finishedAt;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public void setFailureReason(String failureReason) {
        this.failureReason = failureReason;
    }

    public RejectReason getFailureCode() {
        return failureCode;
    }

    public void setFailureCode(RejectReason failureCode) {
        this.failureCode = failureCode;
    }

    public List<String> getPromotedEnvironments() {
        return promotedEnvironments;
    }

    public void setPromotedEnvironments(List<String> promotedEnvironments) {
        this.promotedEnvironments = promotedEnvironments;
    }
}
