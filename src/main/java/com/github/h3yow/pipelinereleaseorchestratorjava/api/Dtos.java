package com.github.h3yow.pipelinereleaseorchestratorjava.api;

import com.github.h3yow.pipelinereleaseorchestratorjava.model.StageSpec;

import java.util.List;

/** Request/response DTOs for the REST surface. */
public final class Dtos {
    private Dtos() {
    }

    public record DefinitionRequest(String name, List<String> environments, List<StageSpec> stages) {
    }

    public record SubmitRunRequest(String versionId, String idempotencyKey, String payload) {
    }

    public record RollbackRequest(String environment, String targetReleaseId, String runId) {
    }

    public record ErrorResponse(String error, String message) {
    }
}
