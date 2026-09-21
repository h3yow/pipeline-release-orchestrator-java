package com.github.h3yow.pipelinereleaseorchestratorjava.service;

import com.github.h3yow.pipelinereleaseorchestratorjava.model.ReleaseRecord;

import java.util.List;

/** Environment promotion, publication and rollback. */
public interface ReleaseService {

    /** Promote a succeeded run to the next environment in declared order (no skip, no re-promotion). */
    ReleaseRecord promote(String runId);

    /** Roll back the current ACTIVE release of an environment to a prior valid release. */
    ReleaseRecord rollback(String pipelineName, String environment,
                           String targetReleaseId, String reason);

    ReleaseRecord currentRelease(String pipelineName, String environment);

    List<ReleaseRecord> history(String pipelineName);
}
