package com.github.h3yow.pipelinereleaseorchestratorjava.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/** Mutable run aggregate. All mutation happens while holding the pipeline lock in PipelineOrchestrator. */
public final class RunRecord {
    private final String runId;
    private final String pipelineId;
    private final String idempotencyKey;
    private final String definitionVersion;
    private final String payloadHash;
    private final Instant createdAt;
    private volatile RunStatus status;
    private volatile String currentEnvironment;
    private volatile int currentStageIndex;
    private volatile int attempt;
    private volatile FailureReason failureReason;
    private volatile String failureMessage;
    private final List<StageExecution> stageExecutions = new ArrayList<>();

    public RunRecord(String runId, String pipelineId, String idempotencyKey, String definitionVersion,
                     String payloadHash, Instant createdAt, RunStatus status, String currentEnvironment,
                     int currentStageIndex, int attempt, FailureReason failureReason,
                     String failureMessage) {
        this(runId, pipelineId, idempotencyKey, definitionVersion, payloadHash, createdAt, status,
                currentEnvironment, currentStageIndex, attempt, failureReason, failureMessage, null);
    }

    @JsonCreator
    public RunRecord(@JsonProperty("runId") String runId, @JsonProperty("pipelineId") String pipelineId,
                     @JsonProperty("idempotencyKey") String idempotencyKey,
                     @JsonProperty("definitionVersion") String definitionVersion,
                     @JsonProperty("payloadHash") String payloadHash,
                     @JsonProperty("createdAt") Instant createdAt,
                     @JsonProperty("status") RunStatus status,
                     @JsonProperty("currentEnvironment") String currentEnvironment,
                     @JsonProperty("currentStageIndex") int currentStageIndex,
                     @JsonProperty("attempt") int attempt,
                     @JsonProperty("failureReason") FailureReason failureReason,
                     @JsonProperty("failureMessage") String failureMessage,
                     @JsonProperty("stageExecutions") List<StageExecution> stageExecutions) {
        this.runId = runId;
        this.pipelineId = pipelineId;
        this.idempotencyKey = idempotencyKey;
        this.definitionVersion = definitionVersion;
        this.payloadHash = payloadHash;
        this.createdAt = createdAt;
        this.status = status;
        this.currentEnvironment = currentEnvironment;
        this.currentStageIndex = currentStageIndex;
        this.attempt = attempt;
        this.failureReason = failureReason;
        this.failureMessage = failureMessage;
        if (stageExecutions != null) {
            this.stageExecutions.addAll(stageExecutions);
        }
    }

    @JsonProperty("runId")
    public String runId() { return runId; }
    @JsonProperty("pipelineId")
    public String pipelineId() { return pipelineId; }
    @JsonProperty("idempotencyKey")
    public String idempotencyKey() { return idempotencyKey; }
    @JsonProperty("definitionVersion")
    public String definitionVersion() { return definitionVersion; }
    @JsonProperty("payloadHash")
    public String payloadHash() { return payloadHash; }
    @JsonProperty("createdAt")
    public Instant createdAt() { return createdAt; }
    @JsonProperty("status")
    public RunStatus status() { return status; }
    public void status(RunStatus s) { this.status = s; }
    @JsonProperty("currentEnvironment")
    public String currentEnvironment() { return currentEnvironment; }
    public void currentEnvironment(String e) { this.currentEnvironment = e; }
    @JsonProperty("currentStageIndex")
    public int currentStageIndex() { return currentStageIndex; }
    public void currentStageIndex(int i) { this.currentStageIndex = i; }
    @JsonProperty("attempt")
    public int attempt() { return attempt; }
    public void attempt(int a) { this.attempt = a; }
    @JsonProperty("failureReason")
    public FailureReason failureReason() { return failureReason; }
    public void failureReason(FailureReason r) { this.failureReason = r; }
    @JsonProperty("failureMessage")
    public String failureMessage() { return failureMessage; }
    public void failureMessage(String m) { this.failureMessage = m; }
    @JsonProperty("stageExecutions")
    public List<StageExecution> stageExecutions() { return stageExecutions; }
}
