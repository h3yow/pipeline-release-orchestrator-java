package com.github.h3yow.pipelinereleaseorchestratorjava.web;

import com.github.h3yow.pipelinereleaseorchestratorjava.model.AuditEvent;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.DefinitionVersion;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.PipelineRun;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.ReleaseRecord;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.AuditService;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.DefinitionService;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.ReleaseService;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.RunService;
import com.github.h3yow.pipelinereleaseorchestratorjava.web.dto.Requests;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** Thin REST facade over the in-process orchestrator services. */
@RestController
public class OrchestratorController {

    private final DefinitionService definitions;
    private final RunService runs;
    private final ReleaseService releases;
    private final AuditService audits;

    public OrchestratorController(DefinitionService definitions, RunService runs,
                                  ReleaseService releases, AuditService audits) {
        this.definitions = definitions;
        this.runs = runs;
        this.releases = releases;
        this.audits = audits;
    }

    // ------------------------------------------------------------- definitions

    @PostMapping("/api/pipelines/{pipeline}/definitions")
    @ResponseStatus(HttpStatus.CREATED)
    public DefinitionVersion createDefinition(@PathVariable String pipeline,
                                              @RequestBody Requests.CreateDefinitionRequest request) {
        return definitions.createVersion(pipeline, request.definition());
    }

    @GetMapping("/api/pipelines/{pipeline}/definitions")
    public List<DefinitionVersion> listDefinitions(@PathVariable String pipeline) {
        return definitions.list(pipeline);
    }

    @GetMapping("/api/pipelines/{pipeline}/definitions/{version}")
    public DefinitionVersion getDefinition(@PathVariable String pipeline,
                                           @PathVariable int version) {
        return definitions.get(pipeline, version);
    }

    @PostMapping("/api/pipelines/{pipeline}/definitions/{version}/withdraw")
    public DefinitionVersion withdraw(@PathVariable String pipeline, @PathVariable int version,
                                      @RequestBody(required = false) Requests.WithdrawRequest request) {
        return definitions.withdraw(pipeline, version, request == null ? null : request.reason());
    }

    // ------------------------------------------------------------- runs

    @PostMapping("/api/pipelines/{pipeline}/runs")
    @ResponseStatus(HttpStatus.CREATED)
    public PipelineRun submit(@PathVariable String pipeline,
                              @RequestBody Requests.SubmitRunRequest request) {
        return runs.submit(pipeline, request.definitionVersion(),
                request.idempotencyKey(), request.payload());
    }

    @GetMapping("/api/runs/{runId}")
    public PipelineRun getRun(@PathVariable String runId) {
        return runs.get(runId);
    }

    @PostMapping("/api/runs/{runId}/advance")
    public PipelineRun advance(@PathVariable String runId) {
        return runs.advance(runId);
    }

    @PostMapping("/api/runs/{runId}/retry")
    public PipelineRun retry(@PathVariable String runId) {
        return runs.retry(runId);
    }

    @PostMapping("/api/runs/{runId}/cancel")
    public PipelineRun cancel(@PathVariable String runId,
                              @RequestBody(required = false) Requests.CancelRequest request) {
        return runs.cancel(runId, request == null ? null : request.reason());
    }

    @PostMapping("/api/runs/{runId}/abort")
    public PipelineRun abort(@PathVariable String runId,
                             @RequestBody(required = false) Requests.AbortRequest request) {
        return runs.abort(runId, request == null ? null : request.reason());
    }

    // ------------------------------------------------------------- releases

    @PostMapping("/api/runs/{runId}/promote")
    @ResponseStatus(HttpStatus.CREATED)
    public ReleaseRecord promote(@PathVariable String runId) {
        return releases.promote(runId);
    }

    @GetMapping("/api/pipelines/{pipeline}/releases")
    public List<ReleaseRecord> releaseHistory(@PathVariable String pipeline) {
        return releases.history(pipeline);
    }

    @PostMapping("/api/pipelines/{pipeline}/environments/{environment}/rollback")
    public ReleaseRecord rollback(@PathVariable String pipeline, @PathVariable String environment,
                                  @RequestBody Requests.RollbackRequest request) {
        return releases.rollback(pipeline, environment, request.targetReleaseId(), request.reason());
    }

    // ------------------------------------------------------------- audit

    @GetMapping("/api/pipelines/{pipeline}/audit")
    public List<AuditEvent> auditTrail(@PathVariable String pipeline) {
        return audits.trail(pipeline);
    }
}
