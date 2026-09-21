package com.github.h3yow.pipelinereleaseorchestratorjava.config;

import com.github.h3yow.pipelinereleaseorchestratorjava.service.DefinitionService;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.PipelineOrchestrator;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.ReleaseService;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.RetryBackoff;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.RunWorker;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.SimulatedStageExecutor;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.StageExecutor;
import com.github.h3yow.pipelinereleaseorchestratorjava.store.AuditLog;
import com.github.h3yow.pipelinereleaseorchestratorjava.store.DefinitionRepository;
import com.github.h3yow.pipelinereleaseorchestratorjava.store.JsonFileStore;
import com.github.h3yow.pipelinereleaseorchestratorjava.store.ReleaseRepository;
import com.github.h3yow.pipelinereleaseorchestratorjava.store.RunRepository;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(OrchestratorProperties.class)
public class OrchestratorConfig {

    @Bean
    JsonFileStore jsonFileStore(OrchestratorProperties properties) {
        return new JsonFileStore(properties.getStorageDir());
    }

    @Bean
    DefinitionRepository definitionRepository(JsonFileStore store) {
        return new DefinitionRepository(store);
    }

    @Bean
    RunRepository runRepository(JsonFileStore store) {
        return new RunRepository(store);
    }

    @Bean
    ReleaseRepository releaseRepository(JsonFileStore store) {
        return new ReleaseRepository(store);
    }

    @Bean
    AuditLog auditLog(JsonFileStore store) {
        return new AuditLog(store);
    }

    @Bean
    RetryBackoff retryBackoff() {
        return new RetryBackoff();
    }

    @Bean
    StageExecutor stageExecutor() {
        return new SimulatedStageExecutor();
    }

    @Bean
    DefinitionService definitionService(DefinitionRepository definitionRepository, AuditLog auditLog) {
        return new DefinitionService(definitionRepository, auditLog);
    }

    @Bean
    ReleaseService releaseService(ReleaseRepository releaseRepository,
                                  DefinitionRepository definitionRepository, AuditLog auditLog) {
        return new ReleaseService(releaseRepository, definitionRepository, auditLog);
    }

    @Bean
    PipelineOrchestrator pipelineOrchestrator(DefinitionService definitionService,
                                              RunRepository runRepository,
                                              ReleaseService releaseService, AuditLog auditLog,
                                              StageExecutor stageExecutor, RetryBackoff retryBackoff,
                                              OrchestratorProperties properties) {
        return new PipelineOrchestrator(definitionService, runRepository, releaseService, auditLog,
                stageExecutor, retryBackoff, properties);
    }

    @Bean
    RunWorker runWorker(PipelineOrchestrator orchestrator) {
        return new RunWorker(orchestrator);
    }
}
