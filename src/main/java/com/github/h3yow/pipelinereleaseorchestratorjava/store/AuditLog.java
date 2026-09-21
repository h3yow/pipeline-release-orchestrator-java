package com.github.h3yow.pipelinereleaseorchestratorjava.store;

import com.github.h3yow.pipelinereleaseorchestratorjava.model.AuditEvent;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.EventType;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.RunStatus;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Append-only audit log with a monotonically increasing global sequence number.
 * Sequence order makes concurrent transitions explainable after the fact.
 */
public class AuditLog {

    private final JsonFileStore store;
    private final AtomicLong sequence = new AtomicLong(0);

    public AuditLog(JsonFileStore store) {
        this.store = store;
        long max = store.readAll("audit", AuditEvent.class).stream()
                .mapToLong(AuditEvent::seq)
                .max()
                .orElse(0L);
        sequence.set(max);
    }

    public AuditEvent append(EventType type, String pipelineId, String runId, String idempotencyKey,
                             String definitionVersion, RunStatus from, RunStatus to, String detail) {
        AuditEvent event = new AuditEvent(sequence.incrementAndGet(), Instant.now(), type, pipelineId,
                runId, idempotencyKey, definitionVersion, from, to, detail);
        store.append("audit", event);
        return event;
    }

    public List<AuditEvent> events(String pipelineId) {
        return store.readAll("audit", AuditEvent.class).stream()
                .filter(e -> pipelineId == null || pipelineId.equals(e.pipelineId()))
                .sorted(Comparator.comparingLong(AuditEvent::seq))
                .toList();
    }
}
