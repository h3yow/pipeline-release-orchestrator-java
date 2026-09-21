package com.github.h3yow.pipelinereleaseorchestratorjava.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

/**
 * An immutable, content-addressed version of a definition.
 * Content ({@code definition}) never changes; only lifecycle status (ACTIVE/WITHDRAWN) may change.
 */
public final class DefinitionVersion {
    private final String pipelineId;
    private final String versionId;
    private final PipelineDefinition definition;
    private final Instant createdAt;
    private volatile DefinitionStatus status;

    @JsonCreator
    public DefinitionVersion(@JsonProperty("pipelineId") String pipelineId,
                             @JsonProperty("versionId") String versionId,
                             @JsonProperty("definition") PipelineDefinition definition,
                             @JsonProperty("createdAt") Instant createdAt,
                             @JsonProperty("status") DefinitionStatus status) {
        this.pipelineId = pipelineId;
        this.versionId = versionId;
        this.definition = definition;
        this.createdAt = createdAt;
        this.status = status;
    }

    @JsonProperty("pipelineId")
    public String pipelineId() { return pipelineId; }

    @JsonProperty("versionId")
    public String versionId() { return versionId; }

    @JsonProperty("definition")
    public PipelineDefinition definition() { return definition; }

    @JsonProperty("createdAt")
    public Instant createdAt() { return createdAt; }

    @JsonProperty("status")
    public DefinitionStatus status() { return status; }

    public void status(DefinitionStatus s) { this.status = s; }
}
