package com.github.h3yow.pipelinereleaseorchestratorjava.model;

import java.time.Instant;

/** Immutable-ish publication record for a released definition version. */
public final class ReleaseRecord {

    private String releaseId;
    private String pipelineName;
    private String runId;
    private int definitionVersion;
    private String environment;
    private ReleaseStatus status;
    private Instant publishedAt;
    private Instant supersededAt;
    private Instant rolledBackAt;
    private String rollbackReason;
    private String supersededByReleaseId;
    private String rolledBackToReleaseId;

    public ReleaseRecord() {
    }

    public String getReleaseId() {
        return releaseId;
    }

    public void setReleaseId(String releaseId) {
        this.releaseId = releaseId;
    }

    public String getPipelineName() {
        return pipelineName;
    }

    public void setPipelineName(String pipelineName) {
        this.pipelineName = pipelineName;
    }

    public String getRunId() {
        return runId;
    }

    public void setRunId(String runId) {
        this.runId = runId;
    }

    public int getDefinitionVersion() {
        return definitionVersion;
    }

    public void setDefinitionVersion(int definitionVersion) {
        this.definitionVersion = definitionVersion;
    }

    public String getEnvironment() {
        return environment;
    }

    public void setEnvironment(String environment) {
        this.environment = environment;
    }

    public ReleaseStatus getStatus() {
        return status;
    }

    public void setStatus(ReleaseStatus status) {
        this.status = status;
    }

    public Instant getPublishedAt() {
        return publishedAt;
    }

    public void setPublishedAt(Instant publishedAt) {
        this.publishedAt = publishedAt;
    }

    public Instant getSupersededAt() {
        return supersededAt;
    }

    public void setSupersededAt(Instant supersededAt) {
        this.supersededAt = supersededAt;
    }

    public Instant getRolledBackAt() {
        return rolledBackAt;
    }

    public void setRolledBackAt(Instant rolledBackAt) {
        this.rolledBackAt = rolledBackAt;
    }

    public String getRollbackReason() {
        return rollbackReason;
    }

    public void setRollbackReason(String rollbackReason) {
        this.rollbackReason = rollbackReason;
    }

    public String getSupersededByReleaseId() {
        return supersededByReleaseId;
    }

    public void setSupersededByReleaseId(String supersededByReleaseId) {
        this.supersededByReleaseId = supersededByReleaseId;
    }

    public String getRolledBackToReleaseId() {
        return rolledBackToReleaseId;
    }

    public void setRolledBackToReleaseId(String rolledBackToReleaseId) {
        this.rolledBackToReleaseId = rolledBackToReleaseId;
    }
}
