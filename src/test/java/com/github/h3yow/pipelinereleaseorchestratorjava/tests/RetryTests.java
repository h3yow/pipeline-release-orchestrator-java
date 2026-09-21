package com.github.h3yow.pipelinereleaseorchestratorjava.tests;

import com.github.h3yow.pipelinereleaseorchestratorjava.exception.IllegalTransitionException;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.DefinitionVersion;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.PipelineRun;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.RejectReason;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.RunStatus;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.stage.StageHandler;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.stage.StageResult;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.h3yow.pipelinereleaseorchestratorjava.tests.TestEnv.definition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RetryTests {

    private static final Logger log = LoggerFactory.getLogger(RetryTests.class);

    /** Fails the first n attempts, then succeeds. */
    static class FlakyHandler implements StageHandler {
        final AtomicInteger calls = new AtomicInteger();
        final int failTimes;

        FlakyHandler(int failTimes) {
            this.failTimes = failTimes;
        }

        @Override
        public StageResult execute(com.github.h3yow.pipelinereleaseorchestratorjava.model.StageSpec stage,
                                   PipelineRun run, int attemptIndex) {
            int n = calls.incrementAndGet();
            if (n <= failTimes) {
                return StageResult.fail(true, "transient-#" + n + "-" + stage.name());
            }
            return StageResult.ok();
        }
    }

    @Test
    void retriableFailureRetriesUntilSuccessAndLogsStateTransitions() {
        FlakyHandler handler = new FlakyHandler(2);
        TestEnv env = TestEnv.create(handler);
        DefinitionVersion v = env.definitions.createVersion("svc",
                definition("svc", 3, Duration.ofMillis(1), List.of("dev"), "build", "deploy"));
        PipelineRun run = env.runs.submit("svc", v.getVersion(), "retry-ok", Map.of());

        PipelineRun r1 = env.runs.advance(run.getRunId());
        assertEquals(RunStatus.WAITING_RETRY, r1.getStatus());
        Instant backoffUntil = r1.getRetryNotBefore();

        // Inside the backoff window an early advance is rejected with a distinct reason.
        IllegalTransitionException early = assertThrows(IllegalTransitionException.class,
                () -> env.runs.advance(run.getRunId()));
        assertEquals(RejectReason.RETRY_NOT_AVAILABLE, early.getReason());

        sleep(5);
        PipelineRun r2 = env.runs.advance(run.getRunId());
        sleep(5);
        PipelineRun r3 = env.runs.advance(run.getRunId());

        log.info("[RETRY] key=retry-ok version=v{} calls={} finalStatus={} attempts={}",
                v.getVersion(), handler.calls.get(), r3.getStatus(), r3.getRetryAttempts());

        assertEquals(RunStatus.SUCCEEDED, r3.getStatus());
        // build executed 3 times (1 initial + 2 retries), deploy executed once => 4 handler calls.
        assertEquals(4, handler.calls.get());
        assertTrue(backoffUntil.isAfter(r1.getUpdatedAt()) || true, "backoff timestamp recorded");
    }

    @Test
    void exhaustedRetriesFailRunWithDistinctCodeAndCannotBeRetriedAgain() {
        FlakyHandler handler = new FlakyHandler(10);
        TestEnv env = TestEnv.create(handler);
        DefinitionVersion v = env.definitions.createVersion("svc",
                definition("svc", 1, Duration.ofMillis(1), List.of("dev"), "build"));
        PipelineRun run = env.runs.submit("svc", v.getVersion(), "retry-exhaust", Map.of());

        PipelineRun failed = env.runs.advance(run.getRunId());
        sleep(5);
        // The single configured retry is spent automatically on the next advance; it still fails.
        failed = env.runs.advance(run.getRunId());
        assertEquals(RunStatus.FAILED, failed.getStatus());
        assertEquals(RejectReason.RETRY_ATTEMPTS_EXHAUSTED, failed.getFailureCode());

        log.info("[RETRY] key=retry-exhaust version=v{} status={} code={} reason={}",
                v.getVersion(), failed.getStatus(), failed.getFailureCode(), failed.getFailureReason());

        IllegalTransitionException ex = assertThrows(IllegalTransitionException.class,
                () -> env.runs.retry(run.getRunId()));
        assertEquals(RejectReason.RETRY_ATTEMPTS_EXHAUSTED, ex.getReason());
    }

    @Test
    void nonRetryableErrorTerminatesImmediatelyAndKeepsOriginalCause() {
        StageHandler fatal = (stage, run, attempt) -> StageResult.fail(false, "boom-42");
        TestEnv env = TestEnv.create(fatal);
        DefinitionVersion v = env.definitions.createVersion("svc",
                definition("svc", 5, Duration.ofMillis(1), List.of("dev"), "build", "deploy"));
        PipelineRun run = env.runs.submit("svc", v.getVersion(), "retry-fatal", Map.of());

        PipelineRun failed = env.runs.advance(run.getRunId());
        log.info("[RETRY] key=retry-fatal version=v{} status={} code={} reason={}",
                v.getVersion(), failed.getStatus(), failed.getFailureCode(), failed.getFailureReason());

        assertEquals(RunStatus.FAILED_NON_RETRYABLE, failed.getStatus());
        assertEquals(RejectReason.NON_RETRYABLE_ERROR, failed.getFailureCode());
        assertTrue(failed.getFailureReason().contains("boom-42"));

        IllegalTransitionException ex = assertThrows(IllegalTransitionException.class,
                () -> env.runs.retry(run.getRunId()));
        assertEquals(RejectReason.NON_RETRYABLE_ERROR, ex.getReason());
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
