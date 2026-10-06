package io.github.hectorvent.floci.runtime.network;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class NetworkPodAdmissionServiceTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void mountsOnlyPublicCaReadOnlyAndInjectsThePathIntoEveryContainer() throws Exception {
        ObjectNode review = MAPPER.createObjectNode();
        ObjectNode request = review.putObject("request").put("uid", "review-123");
        ObjectNode spec = request.putObject("object").putObject("spec");
        spec.putArray("containers").addObject().put("name", "app").put("image", "app:test");
        spec.putArray("initContainers").addObject().put("name", "setup").put("image", "setup:test");
        JsonNode response = MAPPER.valueToTree(NetworkPodAdmission.review(review)).path("response");
        assertTrue(response.path("allowed").asBoolean());
        assertEquals("review-123", response.path("uid").asText());
        JsonNode patch = MAPPER.readTree(Base64.getDecoder().decode(response.path("patch").asText()));
        JsonNode changed = patch.get(0).path("value");
        assertEquals(NetworkIsolationManager.CA_PATH, changed.path("volumes").get(0).path("hostPath").path("path").asText());
        for (String field : List.of("containers", "initContainers")) {
            JsonNode container = changed.path(field).get(0);
            assertEquals("FLOCI_CA_BUNDLE", container.path("env").get(0).path("name").asText());
            assertEquals(NetworkIsolationManager.CA_PATH, container.path("env").get(0).path("value").asText());
            assertTrue(container.path("volumeMounts").get(0).path("readOnly").asBoolean());
        }
        assertFalse(spec.has("volumes"), "Admission must not mutate its input");
    }

    @Test
    void missingSpecAndReservedVariablesFailAdmission() throws Exception {
        assertFalse(MAPPER.valueToTree(NetworkPodAdmission.review(MAPPER.createObjectNode()))
                .path("response").path("allowed").asBoolean());
        JsonNode review = MAPPER.readTree("""
                {"request":{"uid":"123","object":{"spec":{"containers":[{"name":"app","env":[
                  {"name":"FLOCI_CA_BUNDLE","value":"/bad"}]}]}}}}
                """);
        assertFalse(MAPPER.valueToTree(NetworkPodAdmission.review(review)).path("response").path("allowed").asBoolean());
        assertTrue(NetworkPodAdmission.manifest("public-ca").contains("failurePolicy: Fail"));
    }
}
