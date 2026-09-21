package com.github.h3yow.pipelinereleaseorchestratorjava.service;

import com.github.h3yow.pipelinereleaseorchestratorjava.error.DefinitionNotFoundException;
import com.github.h3yow.pipelinereleaseorchestratorjava.error.RollbackNotAllowedException;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.DefinitionStatus;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.DefinitionVersion;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.EventType;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.ReleaseRecord;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.ReleaseStatus;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.ReleaseType;
import com.github.h3yow.pipelinereleaseorchestratorjava.store.AuditLog;
import com.github.h3yow.pipelinereleaseorchestratorjava.store.DefinitionRepository;
import com.github.h3yow.pipelinereleaseorchestratorjava.store.ReleaseRepository;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Publishing and rollback rules:
 *
 * <ul>
 *   <li>Publishing requires an existing ACTIVE definition version and supersedes any previously
 *       active release in that environment (old history rows remain untouched).</li>
 *   <li>A rollback can only act on a release that is still effective (ACTIVE) and has not itself
 *       been superseded. Passing a superseded/unknown release id is rejected with a distinguishable
 *       error. Rolling back the first-ever release (no predecessor) is also rejected.</li>
 *   <li>A rollback restores the definition version of the release immediately preceding the current
 *       one (that version's definition must still be ACTIVE), records a brand-new ROLLBACK release
 *       linked back via {@code relatedReleaseId}, and supersedes the current release. No history is
 *       deleted; the complete order stays explainable from the append-only stream.</li>
 * </ul>
 */
public class ReleaseService {

    private final ReleaseRepository releaseRepository;
    private final DefinitionRepository definitionRepository;
    private final AuditLog auditLog;

    public ReleaseService(ReleaseRepository releaseRepository, DefinitionRepository definitionRepository,
                          AuditLog auditLog) {
        this.releaseRepository = releaseRepository;
        this.definitionRepository = definitionRepository;
        this.auditLog = auditLog;
    }

    public ReleaseRecord publish(String pipelineId, String environment, String versionId, String runId) {
        requireActiveDefinition(pipelineId, versionId);
        String releaseId = "rel-" + UUID.randomUUID();
        Instant now = Instant.now();
        releaseRepository.active(pipelineId, environment).ifPresent(previous ->
                releaseRepository.append(new ReleaseRecord(previous.releaseId(), pipelineId, environment,
                        previous.definitionVersion(), previous.runId(), previous.type(),
                        ReleaseStatus.SUPERSEDED, now, "superseded by " + releaseId,
                        previous.relatedReleaseId())));
        ReleaseRecord record = new ReleaseRecord(releaseId, pipelineId, environment, versionId, runId,
                ReleaseType.DEPLOY, ReleaseStatus.ACTIVE, now, "published from run " + runId, null);
        releaseRepository.append(record);
        auditLog.append(EventType.RELEASE_PUBLISHED, pipelineId, runId, null, versionId,
                null, null, "release " + releaseId + " published to " + environment);
        return record;
    }

    /**
     * Roll back the currently effective release.
     *
     * @param pipelineId     pipeline id
     * @param environment    target environment
     * @param activeReleaseId the ACTIVE release being rolled back (must still be effective)
     * @param runId          optional run that triggered the rollback (for traceability)
     */
    public ReleaseRecord rollback(String pipelineId, String environment, String activeReleaseId,
                                  String runId) {
        List<ReleaseRecord> logicalHistory = releaseRepository.history(pipelineId, environment);
        ReleaseRecord current = releaseRepository.active(pipelineId, environment)
                .orElseThrow(() -> new RollbackNotAllowedException(
                        "no active release exists in " + environment + " for pipeline " + pipelineId));
        if (!current.releaseId().equals(activeReleaseId)) {
            throw new RollbackNotAllowedException(
                    "rollback target release " + activeReleaseId
                            + " is not effective: the current active release is "
                            + current.releaseId() + ". Only the still-active, non-superseded release "
                            + "can be rolled back");
        }
        if (logicalHistory.size() < 2) {
            throw new RollbackNotAllowedException(
                    "cannot roll back release " + activeReleaseId
                            + ": there is no preceding release to restore");
        }
        ReleaseRecord predecessor = logicalHistory.get(logicalHistory.size() - 2);
        requireActiveDefinition(pipelineId, predecessor.definitionVersion());

        Instant now = Instant.now();
        releaseRepository.append(new ReleaseRecord(current.releaseId(), pipelineId, environment,
                current.definitionVersion(), current.runId(), current.type(),
                ReleaseStatus.SUPERSEDED, now,
                "superseded by rollback restoring predecessor " + predecessor.releaseId(),
                current.relatedReleaseId()));
        String releaseId = "rel-" + UUID.randomUUID();
        ReleaseRecord rollbackRecord = new ReleaseRecord(releaseId, pipelineId, environment,
                predecessor.definitionVersion(), runId, ReleaseType.ROLLBACK, ReleaseStatus.ACTIVE, now,
                "rollback from " + current.releaseId() + " restores version "
                        + predecessor.definitionVersion() + " (previous release "
                        + predecessor.releaseId() + ")",
                current.releaseId());
        releaseRepository.append(rollbackRecord);
        auditLog.append(EventType.RELEASE_ROLLED_BACK, pipelineId, runId, null,
                predecessor.definitionVersion(), null, null,
                "rollback release " + releaseId + " from " + current.releaseId() + " restores "
                        + predecessor.releaseId() + " in " + environment);
        return rollbackRecord;
    }

    public List<ReleaseRecord> history(String pipelineId, String environment) {
        return releaseRepository.history(pipelineId, environment);
    }

    private void requireActiveDefinition(String pipelineId, String versionId) {
        DefinitionVersion version = definitionRepository.find(pipelineId, versionId)
                .orElseThrow(() -> new DefinitionNotFoundException(
                        "definition version not found: " + pipelineId + "/" + versionId));
        if (version.status() != DefinitionStatus.ACTIVE) {
            throw new RollbackNotAllowedException(
                    "definition " + versionId + " is withdrawn and cannot be released");
        }
    }
}
