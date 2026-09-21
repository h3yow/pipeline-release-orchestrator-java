package com.github.h3yow.pipelinereleaseorchestratorjava.config;

import com.github.h3yow.pipelinereleaseorchestratorjava.service.stage.AlwaysSucceedStageHandler;
import com.github.h3yow.pipelinereleaseorchestratorjava.service.stage.StageHandler;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(OrchestratorProperties.class)
public class ServiceConfig {

    @Bean
    public StageHandler stageHandler() {
        return new AlwaysSucceedStageHandler();
    }
}
