package com.github.h3yow.pipelinereleaseorchestratorjava.model;

import java.time.Instant;

/**
 * Immutable release history entry.
 *
 * <p>Each logical release keeps the same {@code releaseId} across lifecycle rows: the first row is
 * ACTIVE and a later row with the same id marks it SUPERSEDED. Rollbacks add brand-new ROLLBACK
 * rows (never delete old ones), and {@code relatedReleaseId} links them to the release they
 * supersede, so the complete order is always explainable from the append-only stream.
 */
public record ReleaseRecord(String releaseId, String pipelineId, String environment,
                            String definitionVersion, String runId, ReleaseType type,
                            ReleaseStatus status, Instant publishedAt, String note,
                            String relatedReleaseId) {
}
