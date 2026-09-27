package com.github.h3yow.pipelinereleaseorchestratorjava.model;

import java.time.Instant;

/** An immutable, versioned snapshot of a pipeline definition. */
public final class DefinitionVersion {

    private String pipelineName;
    private int version;
    private PipelineDefinition definition;
    /**
     * Volatile so a drive loop executing stages on another thread observes a
     * withdrawal between stages without waiting for the pipeline lock.
     */
    private volatile DefinitionStatus status;
    private String contentHash;
    private Instant createdAt;
    private Instant withdrawnAt;
    private String withdrawReason;

    public DefinitionVersion() {
    }

    public DefinitionVersion(String pipelineName, int version, PipelineDefinition definition,
                             DefinitionStatus status, String contentHash, Instant createdAt) {
        this.pipelineName = pipelineName;
        this.version = version;
        this.definition = definition;
        this.status = status;
        this.contentHash = contentHash;
        this.createdAt = createdAt;
    }

    public String getPipelineName() {
        return pipelineName;
    }

    public void setPipelineName(String pipelineName) {
        this.pipelineName = pipelineName;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public PipelineDefinition getDefinition() {
        return definition;
    }

    public void setDefinition(PipelineDefinition definition) {
        this.definition = definition;
    }

    public DefinitionStatus getStatus() {
        return status;
    }

    public void setStatus(DefinitionStatus status) {
        this.status = status;
    }

    public String getContentHash() {
        return contentHash;
    }

    public void setContentHash(String contentHash) {
        this.contentHash = contentHash;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getWithdrawnAt() {
        return withdrawnAt;
    }

    public void setWithdrawnAt(Instant withdrawnAt) {
        this.withdrawnAt = withdrawnAt;
    }

    public String getWithdrawReason() {
        return withdrawReason;
    }

    public void setWithdrawReason(String withdrawReason) {
        this.withdrawReason = withdrawReason;
    }

    /** Only ACTIVE versions may be executed; DRAFT/WITHDRAWN are rejected with distinct reasons. */
    public boolean isExecutable() {
        return status == DefinitionStatus.ACTIVE;
    }
}
