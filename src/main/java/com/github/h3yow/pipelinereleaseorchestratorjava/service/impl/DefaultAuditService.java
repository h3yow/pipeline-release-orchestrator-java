package com.github.h3yow.pipelinereleaseorchestratorjava.service.impl;

import com.github.h3yow.pipelinereleaseorchestratorjava.model.AuditEvent;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.AuditService;
import com.github.h3yow.pipelinereleaseorchestratorjava.store.LocalStateStore;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DefaultAuditService implements AuditService {

    private final LocalStateStore store;

    public DefaultAuditService(LocalStateStore store) {
        this.store = store;
    }

    @Override
    public List<AuditEvent> trail(String pipelineName) {
        return store.auditTrail(pipelineName);
    }

    @Override
    public List<AuditEvent> all() {
        return store.allAuditEvents();
    }
}
