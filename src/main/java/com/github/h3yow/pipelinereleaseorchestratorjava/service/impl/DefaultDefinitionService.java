package com.github.h3yow.pipelinereleaseorchestratorjava.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.h3yow.pipelinereleaseorchestratorjava.exception.DefinitionRejectedException;
import com.github.h3yow.pipelinereleaseorchestratorjava.exception.PipelineException;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.AuditType;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.DefinitionStatus;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.DefinitionVersion;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.PipelineDefinition;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.RejectReason;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.StageSpec;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.DefinitionService;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.Hashes;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.PipelineLockManager;
import com.github.h3yow.pipelinereleaseorchestratorjava.store.LocalStateStore;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.NavigableMap;

@Service
public class DefaultDefinitionService implements DefinitionService {

    private final LocalStateStore store;
    private final PipelineLockManager locks;
    private final AuditSupport audit;
    private final ObjectMapper contentMapper;

    public DefaultDefinitionService(LocalStateStore store, PipelineLockManager locks, AuditSupport audit) {
        this.store = store;
        this.locks = locks;
        this.audit = audit;
        this.contentMapper = new ObjectMapper().findAndRegisterModules();
    }

    @Override
    public DefinitionVersion createVersion(String pipelineName, PipelineDefinition definition) {
        validate(pipelineName, definition);
        return locks.withLock(pipelineName, () -> {
            int version = store.nextDefinitionVersion(pipelineName);
            DefinitionVersion dv = new DefinitionVersion(
                    pipelineName, version, definition, DefinitionStatus.ACTIVE,
                    Hashes.sha256(canonicalJson(definition)), Instant.now());
            store.putDefinition(dv);
            store.persistSnapshot();
            audit.emit(AuditType.DEFINITION_CREATED, pipelineName, version,
                    null, null, null, null, null, null);
            return dv;
        });
    }

    @Override
    public DefinitionVersion withdraw(String pipelineName, int version, String reason) {
        return locks.withLock(pipelineName, () -> {
            DefinitionVersion dv = mustFind(pipelineName, version);
            if (dv.getStatus() == DefinitionStatus.WITHDRAWN) {
                return dv;
            }
            dv.setStatus(DefinitionStatus.WITHDRAWN);
            dv.setWithdrawnAt(Instant.now());
            dv.setWithdrawReason(reason);
            store.persistSnapshot();
            audit.emit(AuditType.DEFINITION_WITHDRAWN, pipelineName, version,
                    null, null, null, null, null, reason);
            return dv;
        });
    }

    @Override
    public DefinitionVersion get(String pipelineName, int version) {
        return store.getDefinition(pipelineName, version);
    }

    @Override
    public DefinitionVersion latest(String pipelineName) {
        NavigableMap<Integer, DefinitionVersion> versions = store.getDefinitions(pipelineName);
        return versions.isEmpty() ? null : versions.lastEntry().getValue();
    }

    @Override
    public DefinitionVersion resolveExecutable(String pipelineName, int version) {
        DefinitionVersion dv = store.getDefinition(pipelineName, version);
        if (dv == null) {
            throw new DefinitionRejectedException(RejectReason.DEFINITION_NOT_FOUND,
                    "definition version not found: " + pipelineName + "@v" + version);
        }
        if (dv.getStatus() == DefinitionStatus.WITHDRAWN) {
            throw new DefinitionRejectedException(RejectReason.DEFINITION_WITHDRAWN,
                    "definition version withdrawn: " + pipelineName + "@v" + version
                            + ", reason=" + dv.getWithdrawReason());
        }
        if (!dv.isExecutable()) {
            throw new DefinitionRejectedException(RejectReason.DEFINITION_INVALID,
                    "definition version not executable (status=" + dv.getStatus() + "): "
                            + pipelineName + "@v" + version);
        }
        try {
            validate(pipelineName, dv.getDefinition());
        } catch (PipelineException e) {
            throw new DefinitionRejectedException(RejectReason.DEFINITION_INVALID,
                    "definition content invalid: " + e.getMessage());
        }
        return dv;
    }

    @Override
    public List<DefinitionVersion> list(String pipelineName) {
        return List.copyOf(store.getDefinitions(pipelineName).values());
    }

    private DefinitionVersion mustFind(String pipelineName, int version) {
        DefinitionVersion dv = store.getDefinition(pipelineName, version);
        if (dv == null) {
            throw new DefinitionRejectedException(RejectReason.DEFINITION_NOT_FOUND,
                    "definition version not found: " + pipelineName + "@v" + version);
        }
        return dv;
    }

    private void validate(String pipelineName, PipelineDefinition definition) {
        if (pipelineName == null || pipelineName.isBlank()) {
            throw new DefinitionRejectedException(RejectReason.DEFINITION_INVALID,
                    "pipeline name must not be blank");
        }
        if (definition == null) {
            throw new DefinitionRejectedException(RejectReason.DEFINITION_INVALID,
                    "definition payload must not be null for pipeline " + pipelineName);
        }
        List<StageSpec> stages = definition.getStages();
        if (stages == null || stages.isEmpty()) {
            throw new DefinitionRejectedException(RejectReason.DEFINITION_INVALID,
                    "definition must declare at least one stage: " + pipelineName);
        }
        long distinct = stages.stream().map(StageSpec::name).distinct().count();
        if (distinct != stages.size()) {
            throw new DefinitionRejectedException(RejectReason.DEFINITION_INVALID,
                    "stage names must be unique: " + pipelineName);
        }
        List<String> envs = definition.getPromotionEnvironments();
        if (envs == null) {
            throw new DefinitionRejectedException(RejectReason.DEFINITION_INVALID,
                    "promotion environments list must not be null (may be empty): " + pipelineName);
        }
        if (envs.stream().distinct().count() != envs.size()) {
            throw new DefinitionRejectedException(RejectReason.DEFINITION_INVALID,
                    "promotion environment names must be unique: " + pipelineName);
        }
        if (definition.getRetryPolicy() == null) {
            throw new DefinitionRejectedException(RejectReason.DEFINITION_INVALID,
                    "retry policy must not be null: " + pipelineName);
        }
    }

    private String canonicalJson(PipelineDefinition definition) {
        try {
            return contentMapper.writerWithDefaultPrettyPrinter().writeValueAsString(definition);
        } catch (JsonProcessingException e) {
            throw new DefinitionRejectedException(RejectReason.DEFINITION_INVALID,
                    "failed to canonicalize definition: " + e.getMessage());
        }
    }
}
