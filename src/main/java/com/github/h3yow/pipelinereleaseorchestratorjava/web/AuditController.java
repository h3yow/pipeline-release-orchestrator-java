package com.github.h3yow.pipelinereleaseorchestratorjava.web;

import com.github.h3yow.pipelinereleaseorchestratorjava.model.AuditEvent;
import com.github.h3yow.pipelinereleaseorchestratorjava.store.AuditLog;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class AuditController {

    private final AuditLog auditLog;

    public AuditController(AuditLog auditLog) {
        this.auditLog = auditLog;
    }

    @GetMapping("/api/pipelines/{pipelineId}/audit")
    public List<AuditEvent> pipelineEvents(@PathVariable String pipelineId) {
        return auditLog.events(pipelineId);
    }

    @GetMapping("/api/audit")
    public List<AuditEvent> allEvents(@RequestParam(required = false) String pipelineId) {
        return auditLog.events(pipelineId);
    }
}
