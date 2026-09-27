package com.github.h3yow.pipelinereleaseorchestratorjava.service;

/**
 * Notified when a definition version is withdrawn so in-flight runs bound to that
 * version can be terminated with a distinguishable reason. Implemented by the run
 * service; registered on the definition service to avoid a constructor dependency cycle.
 * Split into a lock-free signal (a long-running stage may be holding the pipeline lock)
 * and a locked convergence step.
 */
public interface DefinitionWithdrawalListener {

    /**
     * Lock-free first step, called before the withdrawal is persisted: register the
     * termination intent on every in-flight run of the version and interrupt any
     * executing stage thread, so the version stops executing as soon as possible.
     */
    void signalDefinitionWithdrawn(String pipelineName, int version, String reason);

    /**
     * Second step, called after the version is marked WITHDRAWN: converge idle runs
     * (PENDING / WAITING_RETRY) to DEFINITION_REVOKED under the pipeline lock; runs
     * currently executing are converged by their own drive loop.
     */
    void convergeWithdrawnRuns(String pipelineName, int version, String reason);
}
