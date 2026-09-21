package com.github.h3yow.pipelinereleaseorchestratorjava.service;

import com.github.h3yow.pipelinereleaseorchestratorjava.model.AuditEvent;

import java.util.List;

/** Read access to the append-only audit trail. */
public interface AuditService {

    List<AuditEvent> trail(String pipelineName);

    List<AuditEvent> all();
}
