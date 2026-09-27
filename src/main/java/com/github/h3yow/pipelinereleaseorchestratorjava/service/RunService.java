package com.github.h3yow.pipelinereleaseorchestratorjava.service;

import com.github.h3yow.pipelinereleaseorchestratorjava.model.PipelineRun;

import java.util.List;
import java.util.Map;

/** Run submission, advancement, retry and cancellation. */
public interface RunService {

    /**
     * Submit a run bound to an explicit, immutable definition version.
     * Repeated submit with the same idempotency key returns the same run;
     * same key with different payload is rejected (never overwritten).
     */
    PipelineRun submit(String pipelineName, int definitionVersion,
                       String idempotencyKey, Map<String, Object> payload);

    /** Advance the run by executing the next pending stage(s) until blocked, failed or finished. */
    PipelineRun advance(String runId);

    /** Retry a failed run if attempts remain; throws for non-retryable / exhausted cases. */
    PipelineRun retry(String runId);

    /** Request cancellation; no-op-ish idempotent for already terminal runs. */
    PipelineRun cancel(String runId, String reason);

    /**
     * Immediately abort an in-flight run: the executing stage is interrupted as soon as
     * possible and the run converges to {@code ABORTED}. Repeated aborts on an already
     * {@code ABORTED} run are idempotent; aborting any other terminal run is rejected
     * with {@code ABORT_NOT_ALLOWED}.
     */
    PipelineRun abort(String runId, String reason);

    PipelineRun get(String runId);

    /**
     * Start queued ({@code PENDING} beyond the in-flight cap) runs of one pipeline when
     * capacity frees up, in FIFO order. Returns the runs started by this pump tick.
     */
    List<PipelineRun> pumpQueued(String pipelineName);
}
