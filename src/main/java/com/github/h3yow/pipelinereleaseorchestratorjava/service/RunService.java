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
     * Immediately abort a run: the in-flight stage is interrupted as soon as
     * possible and the run converges to {@code ABORTED}. Completed stages keep
     * their results; never-executed stages stay PENDING. Repeated aborts are
     * idempotent; aborting an already terminal (non-aborted) run is rejected
     * with {@code ABORT_NOT_ALLOWED}.
     */
    PipelineRun abort(String runId, String reason);

    /**
     * Terminate every non-terminal run bound to a withdrawn definition version.
     * Affected runs converge to {@code INVALIDATED} (distinct from failure and
     * cancellation); already-completed stage records are preserved. Runs already
     * converging to CANCELLED are left to their cancel flow.
     *
     * @return the runs terminated by this call
     */
    List<PipelineRun> invalidateRunsForWithdrawnVersion(String pipelineName, int version, String reason);

    PipelineRun get(String runId);

    /**
     * Start queued ({@code PENDING} beyond the in-flight cap) runs of one pipeline when
     * capacity frees up, in FIFO order. Returns the runs started by this pump tick.
     */
    List<PipelineRun> pumpQueued(String pipelineName);
}
