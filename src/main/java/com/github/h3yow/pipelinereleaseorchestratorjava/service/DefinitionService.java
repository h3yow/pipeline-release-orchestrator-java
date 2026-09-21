package com.github.h3yow.pipelinereleaseorchestratorjava.service;

import com.github.h3yow.pipelinereleaseorchestratorjava.model.DefinitionVersion;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.PipelineDefinition;

import java.util.List;

/** Versioned, immutable pipeline definition management. */
public interface DefinitionService {

    DefinitionVersion createVersion(String pipelineName, PipelineDefinition definition);

    DefinitionVersion withdraw(String pipelineName, int version, String reason);

    DefinitionVersion get(String pipelineName, int version);

    DefinitionVersion latest(String pipelineName);

    /** Resolve and validate that a version exists, is active and legal to execute. */
    DefinitionVersion resolveExecutable(String pipelineName, int version);

    List<DefinitionVersion> list(String pipelineName);
}
