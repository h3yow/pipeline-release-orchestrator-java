package com.github.h3yow.pipelinereleaseorchestratorjava.service;

import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * Serializes every state-machine mutation of one pipeline via a per-pipeline lock,
 * guaranteeing no duplicate advancement, out-of-order transition or partial update
 * under concurrent triggering. Fair ordering gives queued triggers FIFO acquisition.
 */
@Component
public class PipelineLockManager {

    private final ConcurrentHashMap<String, ReentrantLock> locks = new ConcurrentHashMap<>();

    public <T> T withLock(String pipelineName, Supplier<T> action) {
        ReentrantLock lock = lockFor(pipelineName);
        lock.lock();
        try {
            return action.get();
        } finally {
            lock.unlock();
        }
    }

    public void withLock(String pipelineName, Runnable action) {
        withLock(pipelineName, () -> {
            action.run();
            return null;
        });
    }

    ReentrantLock lockFor(String pipelineName) {
        return locks.computeIfAbsent(pipelineName, k -> new ReentrantLock(true));
    }
}
