package com.github.h3yow.pipelinereleaseorchestratorjava.store;

import com.github.h3yow.pipelinereleaseorchestratorjava.model.RunRecord;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Run snapshots are appended to JSONL on every transition (full history is retained on disk).
 * The in-memory map keeps only the latest snapshot per run; the idempotency index is filled at
 * first insert so repeat submissions resolve to the very same run.
 */
public class RunRepository {

    private final JsonFileStore store;
    private final ConcurrentMap<String, RunRecord> byRunId = new ConcurrentHashMap<>();
    // pipelineId + "|" + idempotencyKey -> runId
    private final ConcurrentMap<String, String> idempotencyIndex = new ConcurrentHashMap<>();

    public RunRepository(JsonFileStore store) {
        this.store = store;
    }

    private static String idemKey(String pipelineId, String idempotencyKey) {
        return pipelineId + "|" + idempotencyKey;
    }

    /** Persist current snapshot. First save for a run also registers its idempotency key. */
    public void save(RunRecord run) {
        store.append("runs-" + run.pipelineId(), run);
        byRunId.put(run.runId(), run);
        if (run.idempotencyKey() != null) {
            idempotencyIndex.putIfAbsent(idemKey(run.pipelineId(), run.idempotencyKey()), run.runId());
        }
    }

    public Optional<RunRecord> findById(String runId) {
        return Optional.ofNullable(byRunId.get(runId));
    }

    public Optional<RunRecord> findByIdempotencyKey(String pipelineId, String idempotencyKey) {
        String runId = idempotencyIndex.get(idemKey(pipelineId, idempotencyKey));
        return runId == null ? Optional.empty() : Optional.ofNullable(byRunId.get(runId));
    }

    public List<RunRecord> findByPipeline(String pipelineId) {
        return byRunId.values().stream()
                .filter(r -> r.pipelineId().equals(pipelineId))
                .sorted(java.util.Comparator.comparing(RunRecord::createdAt))
                .toList();
    }
}
