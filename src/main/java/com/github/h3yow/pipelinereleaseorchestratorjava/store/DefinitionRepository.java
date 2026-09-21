package com.github.h3yow.pipelinereleaseorchestratorjava.store;

import com.github.h3yow.pipelinereleaseorchestratorjava.model.DefinitionVersion;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Stores every version snapshot in an append-only JSONL stream (one stream per pipeline).
 * The in-memory index is rebuilt by replaying the stream, so withdrawn status survives restarts.
 */
public class DefinitionRepository {

    private final JsonFileStore store;
    // pipelineId -> (versionId -> latest snapshot)
    private final ConcurrentMap<String, ConcurrentMap<String, DefinitionVersion>> index =
            new ConcurrentHashMap<>();

    public DefinitionRepository(JsonFileStore store) {
        this.store = store;
    }

    private String stream(String pipelineId) {
        return "definitions-" + pipelineId;
    }

    public void put(DefinitionVersion version) {
        store.append(stream(version.pipelineId()), version);
        index.computeIfAbsent(version.pipelineId(), k -> new ConcurrentHashMap<>())
                .put(version.versionId(), version);
    }

    public Optional<DefinitionVersion> find(String pipelineId, String versionId) {
        ConcurrentMap<String, DefinitionVersion> versions = index.get(pipelineId);
        if (versions == null) {
            replay(pipelineId);
            versions = index.get(pipelineId);
        }
        return versions == null ? Optional.empty() : Optional.ofNullable(versions.get(versionId));
    }

    private synchronized void replay(String pipelineId) {
        if (index.containsKey(pipelineId)) {
            return;
        }
        ConcurrentMap<String, DefinitionVersion> map = new ConcurrentHashMap<>();
        for (DefinitionVersion v : store.readAll(stream(pipelineId), DefinitionVersion.class)) {
            map.put(v.versionId(), v);
        }
        index.putIfAbsent(pipelineId, map);
    }

    public List<DefinitionVersion> findAll(String pipelineId) {
        replay(pipelineId);
        ConcurrentMap<String, DefinitionVersion> versions = index.get(pipelineId);
        if (versions == null) {
            return List.of();
        }
        return versions.values().stream()
                .sorted(Comparator.comparing(DefinitionVersion::createdAt))
                .toList();
    }

    public Optional<String> latestVersionId(String pipelineId) {
        return findAll(pipelineId).stream()
                .max(Comparator.comparing(DefinitionVersion::createdAt)
                        .thenComparing(DefinitionVersion::versionId))
                .map(DefinitionVersion::versionId);
    }
}
