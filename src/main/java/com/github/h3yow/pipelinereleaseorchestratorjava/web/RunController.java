package com.github.h3yow.pipelinereleaseorchestratorjava.web;

import com.github.h3yow.pipelinereleaseorchestratorjava.api.Dtos.SubmitRunRequest;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.RunRecord;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.PipelineOrchestrator;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/pipelines/{pipelineId}/runs")
public class RunController {

    private final PipelineOrchestrator orchestrator;

    public RunController(PipelineOrchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }

    @PostMapping
    public RunRecord submit(@PathVariable String pipelineId, @RequestBody SubmitRunRequest request) {
        return orchestrator.submitRun(pipelineId, request.versionId(), request.idempotencyKey(),
                request.payload());
    }

    @GetMapping
    public List<RunRecord> list(@PathVariable String pipelineId) {
        return orchestrator.runs(pipelineId);
    }

    @GetMapping("/{runId}")
    public RunRecord get(@PathVariable String pipelineId, @PathVariable String runId) {
        return orchestrator.getRun(runId);
    }

    @PostMapping("/{runId}/promote")
    public RunRecord promote(@PathVariable String pipelineId, @PathVariable String runId) {
        return orchestrator.promote(runId);
    }

    @PostMapping("/{runId}/retry")
    public RunRecord retry(@PathVariable String pipelineId, @PathVariable String runId) {
        return orchestrator.retry(runId);
    }

    @PostMapping("/{runId}/cancel")
    public RunRecord cancel(@PathVariable String pipelineId, @PathVariable String runId) {
        return orchestrator.cancel(runId);
    }
}
