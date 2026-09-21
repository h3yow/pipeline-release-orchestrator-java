package com.github.h3yow.pipelinereleaseorchestratorjava.tests;

import org.junit.jupiter.api.AfterAll;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Base class that shuts down every harness's worker pool after all tests in a class complete,
 * before JUnit deletes @TempDir directories.
 */
abstract class HarnessCleanup {

    private static final Set<TestHarness> LIVE = ConcurrentHashMap.newKeySet();

    static void register(TestHarness harness) {
        LIVE.add(harness);
    }

    @AfterAll
    static void shutdownAllHarnesses() {
        LIVE.forEach(TestHarness::close);
        LIVE.clear();
    }
}
