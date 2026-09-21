package com.github.h3yow.pipelinereleaseorchestratorjava.service.impl;

import com.github.h3yow.pipelinereleaseorchestratorjava.exception.IllegalTransitionException;
import com.github.h3yow.pipelinereleaseorchestratorjava.exception.PipelineException;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.AuditType;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.DefinitionVersion;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.PipelineRun;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.ReleaseRecord;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.ReleaseStatus;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.RejectReason;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.RunStatus;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.DefinitionService;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.PipelineLockManager;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.ReleaseService;
import com.github.h3yow.pipelinereleaseorchestratorjava.store.LocalStateStore;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Service
public class DefaultReleaseService implements ReleaseService {

    private final LocalStateStore store;
    private final PipelineLockManager locks;
    private final DefinitionService definitions;
    private final AuditSupport audit;

    public DefaultReleaseService(LocalStateStore store, PipelineLockManager locks,
                                 DefinitionService definitions, AuditSupport audit) {
        this.store = store;
        this.locks = locks;
        this.definitions = definitions;
        this.audit = audit;
    }

    @Override
    public ReleaseRecord promote(String runId) {
        PipelineRun found = mustFindRun(runId);
        return locks.withLock(found.getPipelineName(), () -> {
            PipelineRun run = store.getRun(runId);
            if (run.getStatus() != RunStatus.SUCCEEDED) {
                throw new IllegalTransitionException(RejectReason.ENV_NO_ELIGIBLE_RUN,
                        "only SUCCEEDED runs can be promoted; run " + runId + " is " + run.getStatus());
            }
            DefinitionVersion dv = definitions.get(run.getPipelineName(), run.getDefinitionVersion());
            List<String> environments = dv.getDefinition().getPromotionEnvironments();
            if (environments.isEmpty()) {
                throw new IllegalTransitionException(RejectReason.ENV_NO_ELIGIBLE_RUN,
                        "pipeline '" + run.getPipelineName() + "' defines no promotion environments");
            }

            int promoted = run.getPromotedEnvironments().size();
            // Duplicate promotion is rejected: next environment is strictly by declared order.
            if (promoted >= environments.size()) {
                throw new IllegalTransitionException(RejectReason.ENV_ALREADY_PROMOTED,
                        "run " + runId + " has already reached the final environment '"
                                + environments.get(environments.size() - 1) + "'");
            }
            // Skip protection: the Nth promotion always targets the Nth declared environment.
            String environment = environments.get(promoted);

            ReleaseRecord previous = currentRelease(run.getPipelineName(), environment);
            ReleaseRecord record = new ReleaseRecord();
            record.setReleaseId(UUID.randomUUID().toString());
            record.setPipelineName(run.getPipelineName());
            record.setRunId(runId);
            record.setDefinitionVersion(run.getDefinitionVersion());
            record.setEnvironment(environment);
            record.setStatus(ReleaseStatus.ACTIVE);
            record.setPublishedAt(Instant.now());
            store.putRelease(record);

            if (previous != null && previous.getStatus() == ReleaseStatus.ACTIVE) {
                previous.setStatus(ReleaseStatus.SUPERSEDED);
                previous.setSupersededAt(Instant.now());
                previous.setSupersededByReleaseId(record.getReleaseId());
                audit.emit(AuditType.RELEASE_SUPERSEDED, run.getPipelineName(),
                        previous.getDefinitionVersion(), previous.getRunId(),
                        run.getIdempotencyKey(), previous.getReleaseId(), null, null,
                        "superseded in env '" + environment + "' by release " + record.getReleaseId());
            }

            run.getPromotedEnvironments().add(environment);
            store.persistSnapshot();
            audit.emit(AuditType.ENV_PROMOTED, run.getPipelineName(), run.getDefinitionVersion(),
                    runId, run.getIdempotencyKey(), record.getReleaseId(),
                    RunStatus.SUCCEEDED, RunStatus.SUCCEEDED,
                    "promoted to env '" + environment + "' ("
                            + (promoted + 1) + "/" + environments.size() + ")");
            audit.emit(AuditType.RELEASE_PUBLISHED, run.getPipelineName(), run.getDefinitionVersion(),
                    runId, run.getIdempotencyKey(), record.getReleaseId(),
                    null, null, "published v" + run.getDefinitionVersion()
                            + " to env '" + environment + "'");
            return record;
        });
    }

    @Override
    public ReleaseRecord rollback(String pipelineName, String environment,
                                  String targetReleaseId, String reason) {
        return locks.withLock(pipelineName, () -> {
            ReleaseRecord current = currentRelease(pipelineName, environment);
            if (current == null) {
                throw new IllegalTransitionException(RejectReason.RELEASE_NOT_FOUND,
                        "no active release exists for pipeline '" + pipelineName
                                + "' env '" + environment + "'; nothing to roll back");
            }
            ReleaseRecord target = store.getRelease(targetReleaseId);
            if (target == null || !pipelineName.equals(target.getPipelineName())
                    || !environment.equals(target.getEnvironment())) {
                throw new IllegalTransitionException(RejectReason.RELEASE_NOT_FOUND,
                        "rollback target release not found for pipeline '" + pipelineName
                                + "' env '" + environment + "': " + targetReleaseId);
            }
            // The target must be a still-valid published release: ACTIVE or SUPERSEDED rows are
            // preserved history and remain usable rollback anchors; a ROLLED_BACK row is dead and
            // a superseded-by-rollback chain cannot silently drop any history.
            if (target.getStatus() == ReleaseStatus.ROLLED_BACK) {
                throw new IllegalTransitionException(RejectReason.RELEASE_NOT_ACTIVE,
                        "release " + targetReleaseId + " is ROLLED_BACK and is no longer valid; "
                                + "it cannot be a rollback target");
            }
            if (target.getReleaseId().equals(current.getReleaseId())) {
                throw new IllegalTransitionException(RejectReason.RELEASE_NOT_ACTIVE,
                        "release " + targetReleaseId + " is already the current active release");
            }

            Instant now = Instant.now();
            String currentId = current.getReleaseId();
            current.setStatus(ReleaseStatus.ROLLED_BACK);
            current.setRolledBackAt(now);
            current.setRolledBackToReleaseId(target.getReleaseId());
            current.setRollbackReason(reason);

            // The prior release becomes effective again; history rows are never deleted.
            target.setStatus(ReleaseStatus.ACTIVE);
            target.setSupersededAt(null);
            target.setSupersededByReleaseId(null);

            store.persistSnapshot();
            audit.emit(AuditType.RELEASE_ROLLED_BACK, pipelineName,
                    current.getDefinitionVersion(), current.getRunId(), null,
                    currentId, null, null,
                    "rolled back from v" + current.getDefinitionVersion()
                            + " to release " + targetReleaseId
                            + " (v" + target.getDefinitionVersion() + ") in env '"
                            + environment + "': " + reason);
            audit.emit(AuditType.RELEASE_PUBLISHED, pipelineName, target.getDefinitionVersion(),
                    target.getRunId(), null, target.getReleaseId(), null, null,
                    "re-activated after rollback in env '" + environment + "'");
            return target;
        });
    }

    @Override
    public ReleaseRecord currentRelease(String pipelineName, String environment) {
        ReleaseRecord found = null;
        for (ReleaseRecord record : store.listReleases(pipelineName)) {
            if (!environment.equals(record.getEnvironment())) {
                continue;
            }
            if (record.getStatus() == ReleaseStatus.ACTIVE) {
                if (found != null) {
                    throw new PipelineException(RejectReason.CONFLICT,
                            "invariant violated: multiple ACTIVE releases for '"
                                    + pipelineName + "' env '" + environment + "'");
                }
                found = record;
            }
        }
        return found;
    }

    @Override
    public List<ReleaseRecord> history(String pipelineName) {
        return store.listReleases(pipelineName);
    }

    private PipelineRun mustFindRun(String runId) {
        PipelineRun run = store.getRun(runId);
        if (run == null) {
            throw new IllegalTransitionException(RejectReason.RUN_NOT_FOUND, "run not found: " + runId);
        }
        return run;
    }
}
