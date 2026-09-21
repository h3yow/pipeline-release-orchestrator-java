package com.github.h3yow.pipelinereleaseorchestratorjava.store;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.h3yow.pipelinereleaseorchestratorjava.config.OrchestratorProperties;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.AuditEvent;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.DefinitionVersion;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.PipelineRun;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.ReleaseRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-process state with optional local-disk persistence:
 * <ul>
 *   <li>{@code state.json} — atomic full-state snapshot (write-temp + move);</li>
 *   <li>{@code audit.jsonl} — append-only JSON-lines audit trail.</li>
 * </ul>
 * All domain mutations happen under the per-pipeline lock in {@code PipelineLockManager};
 * collections below use concurrent outer maps mainly for safe cross-pipeline reads.
 */
@Component
public class LocalStateStore {

    private static final Logger log = LoggerFactory.getLogger(LocalStateStore.class);

    private final Map<String, TreeMap<Integer, DefinitionVersion>> definitions = new ConcurrentHashMap<>();
    private final Map<String, PipelineRun> runsById = new ConcurrentHashMap<>();
    private final Map<String, List<PipelineRun>> runsByPipeline = new ConcurrentHashMap<>();
    private final Map<String, Map<String, IdempotencyRecord>> idempotency = new ConcurrentHashMap<>();
    private final Map<String, List<ReleaseRecord>> releasesByPipeline = new ConcurrentHashMap<>();
    private final Map<String, ReleaseRecord> releasesById = new ConcurrentHashMap<>();
    private final List<AuditEvent> auditLog = java.util.Collections.synchronizedList(new ArrayList<>());
    private final AtomicLong auditSeq = new AtomicLong(0);

    private final ObjectMapper mapper;
    private final Path storageDir;
    private final boolean persistenceEnabled;

    public LocalStateStore() {
        this(null);
    }

    @Autowired
    public LocalStateStore(OrchestratorProperties properties) {
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE)
                .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        this.mapper.findAndRegisterModules();
        if (properties == null) {
            this.storageDir = null;
            this.persistenceEnabled = false;
        } else {
            this.storageDir = Path.of(properties.getStorageDir());
            this.persistenceEnabled = properties.isPersistenceEnabled();
            if (persistenceEnabled) {
                loadSnapshot();
            }
        }
    }

    // ---------------------------------------------------------------- definitions

    public int nextDefinitionVersion(String pipelineName) {
        TreeMap<Integer, DefinitionVersion> versions = definitions.get(pipelineName);
        return versions == null || versions.isEmpty() ? 1 : versions.lastKey() + 1;
    }

    public void putDefinition(DefinitionVersion version) {
        definitions.computeIfAbsent(version.getPipelineName(), k -> new TreeMap<>())
                .put(version.getVersion(), version);
    }

    public DefinitionVersion getDefinition(String pipelineName, int version) {
        TreeMap<Integer, DefinitionVersion> versions = definitions.get(pipelineName);
        return versions == null ? null : versions.get(version);
    }

    public NavigableMap<Integer, DefinitionVersion> getDefinitions(String pipelineName) {
        TreeMap<Integer, DefinitionVersion> versions = definitions.get(pipelineName);
        return versions == null ? java.util.Collections.emptyNavigableMap()
                : java.util.Collections.unmodifiableNavigableMap(versions);
    }

    // ---------------------------------------------------------------- runs

    public void putRun(PipelineRun run) {
        runsById.put(run.getRunId(), run);
        runsByPipeline.computeIfAbsent(run.getPipelineName(), k -> java.util.Collections.synchronizedList(new ArrayList<>()))
                .add(run);
    }

    public PipelineRun getRun(String runId) {
        return runsById.get(runId);
    }

    public List<PipelineRun> listRuns(String pipelineName) {
        List<PipelineRun> list = runsByPipeline.get(pipelineName);
        if (list == null) {
            return List.of();
        }
        synchronized (list) {
            return List.copyOf(list);
        }
    }

    public long countInFlight(String pipelineName) {
        return listRuns(pipelineName).stream()
                .filter(r -> r.getStatus() != null && r.getStatus().occupiesSlot())
                .count();
    }

    /** Names of pipelines that currently have at least one run. */
    public java.util.Set<String> pipelineNamesWithRuns() {
        return java.util.Set.copyOf(runsByPipeline.keySet());
    }

    // ---------------------------------------------------------------- idempotency

    public void putIdempotencyIndex(String pipelineName, String idempotencyKey,
                                    String runId, String payloadHash) {
        idempotency.computeIfAbsent(pipelineName, k -> new ConcurrentHashMap<>())
                .put(idempotencyKey, new IdempotencyRecord(runId, payloadHash));
    }

    public IdempotencyRecord getIdempotencyRecord(String pipelineName, String idempotencyKey) {
        Map<String, IdempotencyRecord> map = idempotency.get(pipelineName);
        return map == null ? null : map.get(idempotencyKey);
    }

    // ---------------------------------------------------------------- releases

    public void putRelease(ReleaseRecord record) {
        releasesById.put(record.getReleaseId(), record);
        releasesByPipeline.computeIfAbsent(record.getPipelineName(),
                k -> java.util.Collections.synchronizedList(new ArrayList<>())).add(record);
    }

    public ReleaseRecord getRelease(String releaseId) {
        return releasesById.get(releaseId);
    }

    public List<ReleaseRecord> listReleases(String pipelineName) {
        List<ReleaseRecord> list = releasesByPipeline.get(pipelineName);
        if (list == null) {
            return List.of();
        }
        synchronized (list) {
            return List.copyOf(list);
        }
    }

    // ---------------------------------------------------------------- audit

    public void appendAudit(AuditEvent event) {
        event.setSeq(auditSeq.incrementAndGet());
        auditLog.add(event);
        appendAuditToDisk(event);
    }

    public List<AuditEvent> auditTrail(String pipelineName) {
        synchronized (auditLog) {
            return auditLog.stream()
                    .filter(e -> pipelineName.equals(e.getPipelineName()))
                    .sorted(Comparator.comparingLong(AuditEvent::getSeq))
                    .toList();
        }
    }

    public List<AuditEvent> allAuditEvents() {
        synchronized (auditLog) {
            return List.copyOf(auditLog);
        }
    }

    public long nextAuditSeq() {
        return auditSeq.get() + 1;
    }

    // ---------------------------------------------------------------- persistence

    public synchronized void persistSnapshot() {
        if (!persistenceEnabled) {
            return;
        }
        try {
            Files.createDirectories(storageDir);
            Path target = storageDir.resolve("state.json");
            Path tmp = storageDir.resolve("state.json.tmp");
            Snapshot snapshot = new Snapshot();
            snapshot.definitions = new LinkedHashMap<>(definitions);
            snapshot.runsById = new LinkedHashMap<>(runsById);
            snapshot.idempotency = new LinkedHashMap<>(idempotency);
            snapshot.releasesById = new LinkedHashMap<>(releasesById);
            synchronized (auditLog) {
                snapshot.auditLog = new ArrayList<>(auditLog);
            }
            mapper.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(), snapshot);
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to persist state snapshot", e);
        }
    }

    @SuppressWarnings("unchecked")
    public final void loadSnapshot() {
        if (!persistenceEnabled || storageDir == null) {
            return;
        }
        Path stateFile = storageDir.resolve("state.json");
        if (!Files.exists(stateFile)) {
            return;
        }
        try {
            Snapshot snapshot = mapper.readValue(stateFile.toFile(), Snapshot.class);
            if (snapshot.definitions != null) {
                snapshot.definitions.forEach((p, m) ->
                        definitions.put(p, new TreeMap<>(m)));
            }
            if (snapshot.runsById != null) {
                snapshot.runsById.forEach((id, run) -> {
                    runsById.put(id, run);
                    runsByPipeline.computeIfAbsent(run.getPipelineName(),
                            k -> java.util.Collections.synchronizedList(new ArrayList<>())).add(run);
                });
            }
            if (snapshot.idempotency != null) {
                snapshot.idempotency.forEach((p, m) ->
                        idempotency.put(p, new ConcurrentHashMap<>(m)));
            }
            if (snapshot.releasesById != null) {
                snapshot.releasesById.forEach((id, rec) -> {
                    releasesById.put(id, rec);
                    releasesByPipeline.computeIfAbsent(rec.getPipelineName(),
                            k -> java.util.Collections.synchronizedList(new ArrayList<>())).add(rec);
                });
            }
            if (snapshot.auditLog != null) {
                auditLog.addAll(snapshot.auditLog);
                snapshot.auditLog.stream().mapToLong(AuditEvent::getSeq).max()
                        .ifPresent(auditSeq::set);
            }
            log.info("Loaded local state: {} pipeline(s), {} run(s), {} release(s), {} audit event(s)",
                    definitions.size(), runsById.size(), releasesById.size(), auditLog.size());
        } catch (IOException e) {
            throw new UncheckedIOException("failed to load state snapshot", e);
        }
    }

    private void appendAuditToDisk(AuditEvent event) {
        if (!persistenceEnabled) {
            return;
        }
        try {
            Files.createDirectories(storageDir);
            Path file = storageDir.resolve("audit.jsonl");
            try (BufferedWriter w = Files.newBufferedWriter(file, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
                w.write(mapper.writeValueAsString(event));
                w.newLine();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to append audit event", e);
        }
    }

    /** Jackson container for the full snapshot. */
    private static final class Snapshot {
        public Map<String, TreeMap<Integer, DefinitionVersion>> definitions;
        public Map<String, PipelineRun> runsById;
        public Map<String, Map<String, IdempotencyRecord>> idempotency;
        public Map<String, ReleaseRecord> releasesById;
        public List<AuditEvent> auditLog;
    }

    /** Idempotency index record. */
    public record IdempotencyRecord(String runId, String payloadHash) {
    }
}
