package com.github.h3yow.pipelinereleaseorchestratorjava.model;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/** Append-only audit event. */
public final class AuditEvent {

    private long seq;
    private Instant timestamp;
    private AuditType type;
    private String pipelineName;
    private Integer definitionVersion;
    private String runId;
    private String idempotencyKey;
    private String releaseId;
    private RunStatus fromStatus;
    private RunStatus toStatus;
    private String reason;
    private Map<String, String> details = new LinkedHashMap<>();

    public AuditEvent() {
    }

    public long getSeq() {
        return seq;
    }

    public void setSeq(long seq) {
        this.seq = seq;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public AuditType getType() {
        return type;
    }

    public void setType(AuditType type) {
        this.type = type;
    }

    public String getPipelineName() {
        return pipelineName;
    }

    public void setPipelineName(String pipelineName) {
        this.pipelineName = pipelineName;
    }

    public Integer getDefinitionVersion() {
        return definitionVersion;
    }

    public void setDefinitionVersion(Integer definitionVersion) {
        this.definitionVersion = definitionVersion;
    }

    public String getRunId() {
        return runId;
    }

    public void setRunId(String runId) {
        this.runId = runId;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public void setIdempotencyKey(String idempotencyKey) {
        this.idempotencyKey = idempotencyKey;
    }

    public String getReleaseId() {
        return releaseId;
    }

    public void setReleaseId(String releaseId) {
        this.releaseId = releaseId;
    }

    public RunStatus getFromStatus() {
        return fromStatus;
    }

    public void setFromStatus(RunStatus fromStatus) {
        this.fromStatus = fromStatus;
    }

    public RunStatus getToStatus() {
        return toStatus;
    }

    public void setToStatus(RunStatus toStatus) {
        this.toStatus = toStatus;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public Map<String, String> getDetails() {
        return details;
    }

    public void setDetails(Map<String, String> details) {
        this.details = details;
    }
}
