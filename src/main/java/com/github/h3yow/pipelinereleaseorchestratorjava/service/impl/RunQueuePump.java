package com.github.h3yow.pipelinereleaseorchestratorjava.service.impl;

import com.github.h3yow.pipelinereleaseorchestratorjava.service.RunService;
import com.github.h3yow.pipelinereleaseorchestratorjava.store.LocalStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Under the QUEUE overflow policy, periodically starts queued runs in FIFO order as
 * in-flight capacity frees up. Each pipeline is still driven under its own lock, so
 * the pump can never cause concurrent advancement of the same pipeline.
 */
@Component
public class RunQueuePump {

    private static final Logger log = LoggerFactory.getLogger(RunQueuePump.class);

    private final RunService runService;
    private final LocalStateStore store;

    public RunQueuePump(RunService runService, LocalStateStore store) {
        this.runService = runService;
        this.store = store;
    }

    @Scheduled(fixedDelayString = "${orchestrator.queue-pump-delay-ms:100}")
    public void pumpAll() {
        for (String pipelineName : store.pipelineNamesWithRuns()) {
            try {
                runService.pumpQueued(pipelineName);
            } catch (Exception e) {
                log.warn("queue pump tick failed for pipeline '{}': {}", pipelineName, e.toString());
            }
        }
    }
}
