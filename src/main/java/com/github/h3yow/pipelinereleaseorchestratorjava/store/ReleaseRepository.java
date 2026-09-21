package com.github.h3yow.pipelinereleaseorchestratorjava.store;

import com.github.h3yow.pipelinereleaseorchestratorjava.model.ReleaseRecord;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Append-only release history per pipeline. Multiple rows may share a releaseId (ACTIVE then
 * SUPERSEDED); logical releases are reconstructed by taking the last row per releaseId.
 */
public class ReleaseRepository {

    private final JsonFileStore store;

    public ReleaseRepository(JsonFileStore store) {
        this.store = store;
    }

    public void append(ReleaseRecord release) {
        store.append("releases-" + release.pipelineId(), release);
    }

    private List<ReleaseRecord> raw(String pipelineId, String environment) {
        List<ReleaseRecord> out = new ArrayList<>();
        for (ReleaseRecord r : store.readAll("releases-" + pipelineId, ReleaseRecord.class)) {
            if (environment == null || environment.equals(r.environment())) {
                out.add(r);
            }
        }
        return out;
    }

    /** Logical releases in first-seen order; each entry is the latest known row for its id. */
    public List<ReleaseRecord> history(String pipelineId, String environment) {
        Map<String, ReleaseRecord> latest = new LinkedHashMap<>();
        for (ReleaseRecord r : raw(pipelineId, environment)) {
            latest.put(r.releaseId(), r);
        }
        return new ArrayList<>(latest.values());
    }

    /** Every raw row, including SUPERSEDED markers, in append order (for auditing/verification). */
    public List<ReleaseRecord> rawHistory(String pipelineId, String environment) {
        return raw(pipelineId, environment);
    }

    public Optional<ReleaseRecord> active(String pipelineId, String environment) {
        List<ReleaseRecord> logical = history(pipelineId, environment);
        for (int i = logical.size() - 1; i >= 0; i--) {
            ReleaseRecord r = logical.get(i);
            if (r.status() == com.github.h3yow.pipelinereleaseorchestratorjava.model.ReleaseStatus.ACTIVE) {
                return Optional.of(r);
            }
        }
        return Optional.empty();
    }
}
