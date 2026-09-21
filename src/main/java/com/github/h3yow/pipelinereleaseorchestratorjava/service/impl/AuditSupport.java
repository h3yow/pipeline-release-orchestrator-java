package com.github.h3yow.pipelinereleaseorchestratorjava.service.impl;

import com.github.h3yow.pipelinereleaseorchestratorjava.model.AuditEvent;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.AuditType;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.RunStatus;
import com.github.h3yow.pipelinereleaseorchestratorjava.store.LocalStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;

/** Builds, appends and logs audit events in a uniform format. */
@Component
public class AuditSupport {

    private static final Logger auditLog = LoggerFactory.getLogger("pipeline.audit");

    private final LocalStateStore store;

    public AuditSupport(LocalStateStore store) {
        this.store = store;
    }

    public AuditEvent emit(AuditType type, String pipelineName, Integer definitionVersion,
                           String runId, String idempotencyKey, String releaseId,
                           RunStatus from, RunStatus to, String reason) {
        AuditEvent event = new AuditEvent();
        event.setTimestamp(Instant.now());
        event.setType(type);
        event.setPipelineName(pipelineName);
        event.setDefinitionVersion(definitionVersion);
        event.setRunId(runId);
        event.setIdempotencyKey(idempotencyKey);
        event.setReleaseId(releaseId);
        event.setFromStatus(from);
        event.setToStatus(to);
        event.setReason(reason);
        store.appendAudit(event);
        auditLog.info("seq={} type={} pipeline={} version={} runId={} idempotencyKey={} releaseId={} {}->{} reason={}",
                event.getSeq(), type, pipelineName, definitionVersion, runId, idempotencyKey,
                releaseId, from, to, reason);
        return event;
    }
}
