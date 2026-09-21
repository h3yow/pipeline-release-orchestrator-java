package com.github.h3yow.pipelinereleaseorchestratorjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Local-storage and resource-limit configuration. */
@ConfigurationProperties(prefix = "orchestrator")
public class OrchestratorProperties {

    /** Directory for local snapshots and audit log. */
    private String storageDir = "data/orchestrator";

    /** Whether to persist snapshots/audit to local disk. */
    private boolean persistenceEnabled = true;

    /** Max in-flight (non-terminal) runs per pipeline; beyond this new submits are rejected/queued. */
    private int maxInFlightPerPipeline = 16;

    /** Max queued (waiting) runs per pipeline. */
    private int maxQueuedPerPipeline = 64;

    /** REJECT or QUEUE when the in-flight cap is hit. */
    private String overflowPolicy = "REJECT";

    public String getStorageDir() {
        return storageDir;
    }

    public void setStorageDir(String storageDir) {
        this.storageDir = storageDir;
    }

    public boolean isPersistenceEnabled() {
        return persistenceEnabled;
    }

    public void setPersistenceEnabled(boolean persistenceEnabled) {
        this.persistenceEnabled = persistenceEnabled;
    }

    public int getMaxInFlightPerPipeline() {
        return maxInFlightPerPipeline;
    }

    public void setMaxInFlightPerPipeline(int maxInFlightPerPipeline) {
        this.maxInFlightPerPipeline = maxInFlightPerPipeline;
    }

    public int getMaxQueuedPerPipeline() {
        return maxQueuedPerPipeline;
    }

    public void setMaxQueuedPerPipeline(int maxQueuedPerPipeline) {
        this.maxQueuedPerPipeline = maxQueuedPerPipeline;
    }

    public String getOverflowPolicy() {
        return overflowPolicy;
    }

    public void setOverflowPolicy(String overflowPolicy) {
        this.overflowPolicy = overflowPolicy;
    }
}
