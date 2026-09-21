package com.github.h3yow.pipelinereleaseorchestratorjava.config;

import com.github.h3yow.pipelinereleaseorchestratorjava.model.RejectPolicy;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "orchestrator")
public class OrchestratorProperties {
    private String storageDir = "data/pipeline";
    private int maxConcurrentRuns = 4;
    private int queueCapacity = 100;
    private RejectPolicy rejectPolicy = RejectPolicy.QUEUE;
    private int defaultMaxAttempts = 3;
    private long defaultInitialBackoffMillis = 50;
    private long defaultMaxBackoffMillis = 5000;

    public String getStorageDir() { return storageDir; }
    public void setStorageDir(String storageDir) { this.storageDir = storageDir; }
    public int getMaxConcurrentRuns() { return maxConcurrentRuns; }
    public void setMaxConcurrentRuns(int v) { this.maxConcurrentRuns = v; }
    public int getQueueCapacity() { return queueCapacity; }
    public void setQueueCapacity(int v) { this.queueCapacity = v; }
    public RejectPolicy getRejectPolicy() { return rejectPolicy; }
    public void setRejectPolicy(RejectPolicy v) { this.rejectPolicy = v; }
    public int getDefaultMaxAttempts() { return defaultMaxAttempts; }
    public void setDefaultMaxAttempts(int v) { this.defaultMaxAttempts = v; }
    public long getDefaultInitialBackoffMillis() { return defaultInitialBackoffMillis; }
    public void setDefaultInitialBackoffMillis(long v) { this.defaultInitialBackoffMillis = v; }
    public long getDefaultMaxBackoffMillis() { return defaultMaxBackoffMillis; }
    public void setDefaultMaxBackoffMillis(long v) { this.defaultMaxBackoffMillis = v; }
}
