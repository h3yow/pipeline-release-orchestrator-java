package com.github.h3yow.pipelinereleaseorchestratorjava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.PipelineDefinition;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.PipelineRun;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.ReleaseRecord;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.RetryPolicy;
import com.github.h3yow.pipelinereleaseorchestratorjava.model.StageSpec;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class WebApiTests {

    @Autowired
    private MockMvc mvc;
    private final ObjectMapper json = new ObjectMapper().findAndRegisterModules();

    private PipelineDefinition definition() {
        return new PipelineDefinition("web",
                List.of(new StageSpec("build", true), new StageSpec("test", true)),
                List.of("dev", "prod"),
                new RetryPolicy(1, Duration.ofMillis(1), 2.0, Duration.ofSeconds(1)));
    }

    @Test
    void fullHappyPathOverHttpAndDistinctRejectionCodes() throws Exception {
        // create definition v1
        String created = mvc.perform(post("/api/pipelines/web/definitions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsBytes(Map.of("definition", definition()))))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.version").value(1))
                .andExpect(jsonPath("$.status").value("ACTIVE"))
                .andReturn().getResponse().getContentAsString();
        int version = json.readTree(created).get("version").asInt();

        // submit + duplicate
        var submitBody = Map.of("definitionVersion", version, "idempotencyKey", "web-k1",
                "payload", Map.of("commit", "c1"));
        String runJson = mvc.perform(post("/api/pipelines/web/runs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsBytes(submitBody)))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        String runId = json.readTree(runJson).get("runId").asText();

        mvc.perform(post("/api/pipelines/web/runs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsBytes(submitBody)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.runId").value(runId));

        // same key, different payload -> 409 CONFLICT + reason
        var clashBody = Map.of("definitionVersion", version, "idempotencyKey", "web-k1",
                "payload", Map.of("commit", "DIFFERENT"));
        mvc.perform(post("/api/pipelines/web/runs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsBytes(clashBody)))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.reason").value("IDEMPOTENT_KEY_PAYLOAD_MISMATCH"));

        // advance -> success
        mvc.perform(post("/api/runs/{id}/advance", runId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCEEDED"));

        // promote dev then prod, third is duplicate -> 422
        String rel1 = mvc.perform(post("/api/runs/{id}/promote", runId))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.environment").value("dev"))
                .andReturn().getResponse().getContentAsString();
        String devReleaseId = json.readTree(rel1).get("releaseId").asText();

        mvc.perform(post("/api/runs/{id}/promote", runId))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.environment").value("prod"));
        mvc.perform(post("/api/runs/{id}/promote", runId))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.reason").value("ENV_ALREADY_PROMOTED"));

        // withdrawn version cannot run
        mvc.perform(post("/api/pipelines/web/definitions/{v}/withdraw", version)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsBytes(Map.of("reason", "nope"))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("WITHDRAWN"));
        mvc.perform(post("/api/pipelines/web/runs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsBytes(Map.of("definitionVersion", version,
                                "idempotencyKey", "web-k2", "payload", Map.of()))))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.reason").value("DEFINITION_WITHDRAWN"));

        // audit trail present
        mvc.perform(get("/api/pipelines/web/audit"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].seq").exists());
    }

    @Test
    void abortAndWithdrawalSemanticsOverHttp() throws Exception {
        String created = mvc.perform(post("/api/pipelines/web2/definitions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsBytes(Map.of("definition", definition()))))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        int version = json.readTree(created).get("version").asInt();

        // abort a PENDING run -> ABORTED with distinguishable code; repeated abort idempotent
        var submitBody = Map.of("definitionVersion", version, "idempotencyKey", "web-abort",
                "payload", Map.of());
        String runJson = mvc.perform(post("/api/pipelines/web2/runs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsBytes(submitBody)))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        String runId = json.readTree(runJson).get("runId").asText();

        mvc.perform(post("/api/runs/{id}/abort", runId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsBytes(Map.of("reason", "halt now"))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("ABORTED"))
                .andExpect(jsonPath("$.failureCode").value("RUN_ABORTED"))
                .andExpect(jsonPath("$.failureReason").value("halt now"));
        mvc.perform(post("/api/runs/{id}/abort", runId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsBytes(Map.of("reason", "again"))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("ABORTED"))
                .andExpect(jsonPath("$.failureReason").value("halt now"));

        // aborting a SUCCEEDED run -> 422 ABORT_NOT_ALLOWED
        var submit2 = Map.of("definitionVersion", version, "idempotencyKey", "web-ok",
                "payload", Map.of());
        String run2Json = mvc.perform(post("/api/pipelines/web2/runs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsBytes(submit2)))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        String run2Id = json.readTree(run2Json).get("runId").asText();
        mvc.perform(post("/api/runs/{id}/advance", run2Id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCEEDED"));
        mvc.perform(post("/api/runs/{id}/abort", run2Id)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsBytes(Map.of("reason", "too late"))))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.reason").value("ABORT_NOT_ALLOWED"));

        // withdrawing the version revokes the in-flight run with a distinct terminal state
        var submit3 = Map.of("definitionVersion", version, "idempotencyKey", "web-revoke",
                "payload", Map.of());
        String run3Json = mvc.perform(post("/api/pipelines/web2/runs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsBytes(submit3)))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        String run3Id = json.readTree(run3Json).get("runId").asText();
        mvc.perform(post("/api/pipelines/web2/definitions/{v}/withdraw", version)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsBytes(Map.of("reason", "recall"))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("WITHDRAWN"));
        mvc.perform(get("/api/runs/{id}", run3Id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("DEFINITION_REVOKED"))
                .andExpect(jsonPath("$.failureCode").value("DEFINITION_WITHDRAWN"))
                .andExpect(jsonPath("$.failureReason").value("recall"));
    }
}
