package io.github.hectorvent.floci.services.rds.container;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.runtime.network.NativeNetworkPolicy;
import jakarta.enterprise.context.ApplicationScoped;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/** Connects to the externally managed native host helper. Never launches a Linux container on macOS. */
@ApplicationScoped
public class MicrosandboxSidecarManager {
    private final EmulatorConfig config;
    private final ObjectMapper mapper;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();

    public MicrosandboxSidecarManager(EmulatorConfig config, ObjectMapper mapper) {
        this.config = config;
        this.mapper = mapper;
    }

    String url() {
        String url = config.services().rds().microsandboxUrl().orElseThrow(() ->
                new IllegalStateException("Set FLOCI_SERVICES_RDS_MICROSANDBOX_URL for the native host helper"));
        return url.replaceAll("/+$", "");
    }

    String token() {
        return config.services().rds().microsandboxToken().filter(value -> !value.isBlank()).orElseThrow(() ->
                new IllegalStateException("Set FLOCI_SERVICES_RDS_MICROSANDBOX_TOKEN"));
    }

    String networkPolicy() {
        return NativeNetworkPolicy.expectedFingerprint(config);
    }

    public boolean isAvailable() {
        token();
        HttpRequest request = HttpRequest.newBuilder(URI.create(url() + "/health"))
                .timeout(Duration.ofSeconds(3)).GET().build();
        try {
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                return false;
            }
            JsonNode health = mapper.readTree(response.body());
            String expectedPolicy = networkPolicy();
            if (!expectedPolicy.isBlank() && !expectedPolicy.equals(health.path("networkPolicy").asText())) {
                throw new IllegalStateException("Microsandbox helper network policy does not match Floci");
            }
            if (!"1".equals(health.path("contract").asText())
                    || !"microsandbox".equals(health.path("name").asText())) {
                throw new IllegalStateException("Incompatible microsandbox helper at " + url()
                        + "; expected microsandbox health contract 1");
            }
            return "ok".equals(health.path("status").asText());
        } catch (IOException e) {
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted checking microsandbox helper", e);
        }
    }
}
