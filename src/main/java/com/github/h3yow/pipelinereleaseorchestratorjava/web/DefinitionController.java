package com.github.h3yow.pipelinereleaseorchestratorjava.web;

import com.github.h3yow.pipelinereleaseorchestratorjava.api.Dtos.DefinitionRequest;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.DefinitionVersion;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.PipelineDefinition;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.DefinitionService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/pipelines")
public class DefinitionController {

    private final DefinitionService definitionService;

    public DefinitionController(DefinitionService definitionService) {
        this.definitionService = definitionService;
    }

    /** Create a new immutable version; identical content returns the same version id. */
    @PostMapping("/{pipelineId}/definitions")
    public DefinitionVersion create(@PathVariable String pipelineId,
                                    @RequestBody DefinitionRequest request) {
        return definitionService.createOrGet(pipelineId,
                new PipelineDefinition(request.name(), request.environments(), request.stages()));
    }

    @GetMapping("/{pipelineId}/definitions")
    public List<DefinitionVersion> versions(@PathVariable String pipelineId) {
        return definitionService.versions(pipelineId);
    }

    @GetMapping("/{pipelineId}/definitions/{versionId}")
    public DefinitionVersion version(@PathVariable String pipelineId,
                                     @PathVariable String versionId) {
        return definitionService.getVersion(pipelineId, versionId);
    }

    @PostMapping("/{pipelineId}/definitions/{versionId}/withdraw")
    public DefinitionVersion withdraw(@PathVariable String pipelineId,
                                      @PathVariable String versionId) {
        definitionService.withdraw(pipelineId, versionId);
        return definitionService.getVersion(pipelineId, versionId);
    }
}
