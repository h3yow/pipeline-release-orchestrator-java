package com.github.h3yow.pipelinereleaseorchestratorjava.web.dto;

import com.github.h3yow.pipelinereleaseorchestratorjava.model.PipelineDefinition;

import java.util.Map;

/** Request DTOs for the orchestrator REST endpoints. */
public final class Requests {

    private Requests() {
    }

    public record CreateDefinitionRequest(PipelineDefinition definition) {
    }

    public record WithdrawRequest(String reason) {
    }

    public record SubmitRunRequest(int definitionVersion, String idempotencyKey,
                                   Map<String, Object> payload) {
    }

    public record CancelRequest(String reason) {
    }

    public record RollbackRequest(String targetReleaseId, String reason) {
    }
}
