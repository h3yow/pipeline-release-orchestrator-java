package com.github.h3yow.pipelinereleaseorchestratorjava.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * Append-only JSONL local storage. Each named stream maps to {@code <baseDir>/<stream>.jsonl}.
 * Every mutation is a single appended line, so history is never silently rewritten or dropped.
 */
public class JsonFileStore {

    private final Path baseDir;
    private final ObjectMapper mapper;
    private final Object writeLock = new Object();

    public JsonFileStore(String baseDir) {
        this.baseDir = Path.of(baseDir);
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        try {
            Files.createDirectories(this.baseDir);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot create storage dir " + baseDir, e);
        }
    }

    private Path file(String stream) {
        return baseDir.resolve(stream.replace('/', '_') + ".jsonl");
    }

    public void append(String stream, Object event) {
        synchronized (writeLock) {
            try {
                String line = mapper.writeValueAsString(event);
                Files.writeString(file(stream), line + System.lineSeparator(), StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException e) {
                throw new UncheckedIOException("append failed for stream " + stream, e);
            }
        }
    }

    public <T> List<T> readAll(String stream, Class<T> type) {
        Path path = file(stream);
        if (!Files.exists(path)) {
            return new ArrayList<>();
        }
        try {
            List<T> result = new ArrayList<>();
            for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
                if (!line.isBlank()) {
                    result.add(mapper.readValue(line, type));
                }
            }
            return result;
        } catch (IOException e) {
            throw new UncheckedIOException("read failed for stream " + stream, e);
        }
    }
}
