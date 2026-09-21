package com.github.h3yow.pipelinereleaseorchestratorjava.model;

import java.time.Instant;

/** One append-only audit event. */
public record AuditEvent(long seq, Instant at, EventType type, String pipelineId,
                         String runId, String idempotencyKey, String definitionVersion,
                         RunStatus fromStatus, RunStatus toStatus, String detail) {
}
