package io.github.hectorvent.floci.runtime.network;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/** Adds only a public trust anchor to containers in Floci-managed k3s clusters. */
final class NetworkPodAdmission {
    static final String HOST = "network.floci.internal";
    static final int PORT = 9443;
    static final String PATH = "/_floci/network/pods";
    private static final String VOLUME = "floci-network-ca";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private NetworkPodAdmission() {}

    static Map<String, Object> review(JsonNode review) {
        JsonNode request = review.path("request");
        String uid = request.path("uid").asText();
        ObjectNode response = MAPPER.createObjectNode().put("uid", uid);
        try {
            JsonNode original = request.path("object").path("spec");
            if (!(original instanceof ObjectNode)) {
                throw new IllegalArgumentException("Expected a Pod spec");
            }
            ObjectNode spec = original.deepCopy();
            ArrayNode volumes = array(spec, "volumes");
            if (named(volumes, VOLUME)) {
                throw new IllegalArgumentException("Pod uses the reserved Floci CA volume name");
            }
            volumes.addObject().put("name", VOLUME).putObject("hostPath")
                    .put("path", NetworkIsolationManager.CA_PATH).put("type", "File");
            for (String field : List.of("containers", "initContainers")) {
                for (JsonNode node : spec.path(field)) {
                    if (!(node instanceof ObjectNode container)) {
                        throw new IllegalArgumentException("Invalid pod container");
                    }
                    ArrayNode env = array(container, "env");
                    if (named(env, NetworkIsolationManager.CA_ENV)) {
                        throw new IllegalArgumentException("Pod overrides the reserved FLOCI_CA_BUNDLE variable");
                    }
                    env.addObject().put("name", NetworkIsolationManager.CA_ENV)
                            .put("value", NetworkIsolationManager.CA_PATH);
                    ArrayNode mounts = array(container, "volumeMounts");
                    for (JsonNode mount : mounts) {
                        String path = mount.path("mountPath").asText();
                        if (NetworkIsolationManager.CA_PATH.equals(path)) {
                            throw new IllegalArgumentException("Pod overrides the Floci CA mount path");
                        }
                    }
                    mounts.addObject().put("name", VOLUME).put("mountPath", NetworkIsolationManager.CA_PATH)
                            .put("readOnly", true);
                }
            }
            byte[] patch = MAPPER.writeValueAsBytes(List.of(Map.of("op", "replace", "path", "/spec", "value", spec)));
            response.put("allowed", true).put("patchType", "JSONPatch")
                    .put("patch", Base64.getEncoder().encodeToString(patch));
        } catch (Exception e) {
            response.put("allowed", false).putObject("status").put("message", e.getMessage());
        }
        return Map.of("apiVersion", "admission.k8s.io/v1", "kind", "AdmissionReview", "response", response);
    }

    private static ArrayNode array(ObjectNode node, String field) {
        if (!node.has(field)) {
            return node.putArray(field);
        }
        if (node.get(field) instanceof ArrayNode array) {
            return array;
        }
        throw new IllegalArgumentException(field + " must be an array");
    }

    private static boolean named(ArrayNode entries, String name) {
        for (JsonNode entry : entries) {
            if (name.equals(entry.path("name").asText())) {
                return true;
            }
        }
        return false;
    }

    static String manifest(String caPem) {
        return """
                apiVersion: admissionregistration.k8s.io/v1
                kind: MutatingWebhookConfiguration
                metadata:
                  name: floci-network-trust
                webhooks:
                  - name: network.floci.io
                    admissionReviewVersions: ["v1"]
                    sideEffects: None
                    failurePolicy: Fail
                    reinvocationPolicy: Never
                    timeoutSeconds: 5
                    clientConfig:
                      url: "https://%s:%d%s"
                      caBundle: "%s"
                    rules:
                      - operations: ["CREATE"]
                        apiGroups: [""]
                        apiVersions: ["v1"]
                        resources: ["pods"]
                        scope: "*"
                """.formatted(HOST, PORT, PATH, Base64.getEncoder().encodeToString(caPem.getBytes(StandardCharsets.US_ASCII)));
    }
}
