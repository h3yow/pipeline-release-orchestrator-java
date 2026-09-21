package com.github.h3yow.pipelinereleaseorchestratorjava.web;

import com.github.h3yow.pipelinereleaseorchestratorjava.api.Dtos.RollbackRequest;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.ReleaseRecord;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.ReleaseService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class ReleaseController {

    private final ReleaseService releaseService;

    public ReleaseController(ReleaseService releaseService) {
        this.releaseService = releaseService;
    }

    @GetMapping("/api/pipelines/{pipelineId}/releases")
    public List<ReleaseRecord> history(@PathVariable String pipelineId,
                                       @RequestParam String environment) {
        return releaseService.history(pipelineId, environment);
    }

    /** Roll back to a release that is still active (not superseded). */
    @PostMapping("/api/pipelines/{pipelineId}/rollbacks")
    public ReleaseRecord rollback(@PathVariable String pipelineId,
                                  @RequestBody RollbackRequest request) {
        return releaseService.rollback(pipelineId, request.environment(),
                request.targetReleaseId(), request.runId());
    }
}
