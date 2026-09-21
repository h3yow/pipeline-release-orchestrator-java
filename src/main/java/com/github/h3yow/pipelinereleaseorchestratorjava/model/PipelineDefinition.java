package com.github.h3yow.pipelinereleaseorchestratorjava.model;

import java.util.List;

/** Immutable definition payload (shared by all versions of a pipeline name). */
public final class PipelineDefinition {

    private String name;
    private List<StageSpec> stages;
    private List<String> promotionEnvironments;
    private RetryPolicy retryPolicy;

    public PipelineDefinition() {
    }

    public PipelineDefinition(String name, List<StageSpec> stages,
                              List<String> promotionEnvironments, RetryPolicy retryPolicy) {
        this.name = name;
        this.stages = stages;
        this.promotionEnvironments = promotionEnvironments;
        this.retryPolicy = retryPolicy;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<StageSpec> getStages() {
        return stages;
    }

    public void setStages(List<StageSpec> stages) {
        this.stages = stages;
    }

    public List<String> getPromotionEnvironments() {
        return promotionEnvironments;
    }

    public void setPromotionEnvironments(List<String> promotionEnvironments) {
        this.promotionEnvironments = promotionEnvironments;
    }

    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    public void setRetryPolicy(RetryPolicy retryPolicy) {
        this.retryPolicy = retryPolicy;
    }
}
