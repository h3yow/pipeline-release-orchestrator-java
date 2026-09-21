package com.github.h3yow.pipelinereleaseorchestratorjava.service;

import com.github.h3yow.pipelinereleaseorchestratorjava.error.DefinitionNotFoundException;
import com.github.h3yow.pipelinereleaseorchestratorjava.error.InvalidDefinitionException;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.DefinitionStatus;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.DefinitionVersion;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.EventType;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.PipelineDefinition;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.StageSpec;
import com.github.h3yow.pipelinereleaseorchestratorjava.store.AuditLog;
import com.github.h3yow.pipelinereleaseorchestratorjava.store.DefinitionRepository;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;

/**
 * Version management for pipeline definitions.
 *
 * <p>Version ids are content-addressed (sha256 of the canonical definition content), so submitting
 * the same definition twice returns the same immutable version. Different content always yields a
 * new version. Runs bind to the exact version id; a withdrawn or non-existent version can never run.
 */
public class DefinitionService {

    private final DefinitionRepository repository;
    private final AuditLog auditLog;

    public DefinitionService(DefinitionRepository repository, AuditLog auditLog) {
        this.repository = repository;
        this.auditLog = auditLog;
    }

    public DefinitionVersion createOrGet(String pipelineId, PipelineDefinition definition) {
        validate(pipelineId, definition);
        String versionId = "v-" + contentHash(pipelineId, definition);
        return repository.find(pipelineId, versionId).orElseGet(() -> {
            DefinitionVersion version = new DefinitionVersion(pipelineId, versionId, definition,
                    Instant.now(), DefinitionStatus.ACTIVE);
            repository.put(version);
            auditLog.append(EventType.DEFINITION_CREATED, pipelineId, null, null, versionId,
                    null, null, "definition created with " + definition.stages().size()
                            + " stages and " + definition.environments().size() + " environments");
            return version;
        });
    }

    public DefinitionVersion getVersion(String pipelineId, String versionId) {
        return repository.find(pipelineId, versionId)
                .orElseThrow(() -> new DefinitionNotFoundException(
                        "definition version not found: " + pipelineId + "/" + versionId));
    }

    public List<DefinitionVersion> versions(String pipelineId) {
        return repository.findAll(pipelineId);
    }

    public void withdraw(String pipelineId, String versionId) {
        DefinitionVersion version = getVersion(pipelineId, versionId);
        if (version.status() == DefinitionStatus.WITHDRAWN) {
            return;
        }
        version.status(DefinitionStatus.WITHDRAWN);
        repository.put(version);
        auditLog.append(EventType.DEFINITION_WITHDRAWN, pipelineId, null, null, versionId,
                null, null, "definition withdrawn");
    }

    private void validate(String pipelineId, PipelineDefinition d) {
        if (pipelineId == null || pipelineId.isBlank()) {
            throw new InvalidDefinitionException("pipelineId must not be blank");
        }
        if (d == null) {
            throw new InvalidDefinitionException("definition must not be null");
        }
        if (d.name() == null || d.name().isBlank()) {
            throw new InvalidDefinitionException("definition name must not be blank");
        }
        if (d.environments().isEmpty()) {
            throw new InvalidDefinitionException("definition must declare at least one environment");
        }
        if (d.environments().stream().distinct().count() != d.environments().size()) {
            throw new InvalidDefinitionException("environments must not contain duplicates");
        }
        if (d.stages().isEmpty()) {
            throw new InvalidDefinitionException("definition must declare at least one stage");
        }
        for (StageSpec stage : d.stages()) {
            if (stage.name() == null || stage.name().isBlank()) {
                throw new InvalidDefinitionException("stage name must not be blank");
            }
            if (stage.environments().isEmpty()) {
                throw new InvalidDefinitionException(
                        "stage '" + stage.name() + "' must apply to at least one environment");
            }
            for (String env : stage.environments()) {
                if (!d.environments().contains(env)) {
                    throw new InvalidDefinitionException(
                            "stage '" + stage.name() + "' references unknown environment: " + env);
                }
            }
        }
        long distinctStageNames = d.stages().stream().map(StageSpec::name).distinct().count();
        if (distinctStageNames != d.stages().size()) {
            throw new InvalidDefinitionException("stage names must be unique");
        }
    }

    private String contentHash(String pipelineId, PipelineDefinition d) {
        StringBuilder canonical = new StringBuilder(pipelineId).append('|').append(d.name()).append('|');
        canonical.append(String.join(",", d.environments()));
        for (StageSpec s : d.stages()) {
            canonical.append('|').append(s.name())
                    .append(':').append(s.retryable())
                    .append(':').append(String.join(",", s.environments()));
        }
        String sha = Idempotency.sha256(canonical.toString());
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(sha.getBytes(StandardCharsets.UTF_8))
                .substring(0, 22);
    }
}
