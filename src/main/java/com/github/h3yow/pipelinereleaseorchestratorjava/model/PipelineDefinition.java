package com.github.h3yow.pipelinereleaseorchestratorjava.model;

import java.util.List;

/** Immutable content of a pipeline definition. */
public record PipelineDefinition(String name, List<String> environments, List<StageSpec> stages) {
    public PipelineDefinition {
        environments = environments == null ? List.of() : List.copyOf(environments);
        stages = stages == null ? List.of() : List.copyOf(stages);
    }
}
