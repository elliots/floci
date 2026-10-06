package io.github.hectorvent.floci.runtime.ondemand;

import io.github.hectorvent.floci.core.common.SsrfProtection;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

final class HttpReadiness implements ActivationCoordinator.Readiness, AutoCloseable {
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    @Override
    public void await(WorkloadDefinition definition, URI backend, Duration timeout) throws Exception {
        if (definition.readinessPath() == null) {
            return;
        }
        SsrfProtection.rejectMetadataAddresses(InetAddress.getAllByName(backend.getHost()), backend.getHost());
        long deadline = System.nanoTime() + timeout.toNanos();
        Throwable lastFailure = null;
        while (System.nanoTime() < deadline) {
            Duration remaining = Duration.ofNanos(deadline - System.nanoTime());
            if (remaining.isNegative() || remaining.isZero()) {
                break;
            }
            Duration requestTimeout = remaining.compareTo(Duration.ofSeconds(5)) < 0 ? remaining : Duration.ofSeconds(5);
            HttpRequest request = HttpRequest.newBuilder(backend.resolve(definition.readinessPath()))
                    .timeout(requestTimeout).GET().build();
            try {
                HttpResponse<Void> response = client.send(request, HttpResponse.BodyHandlers.discarding());
                if (response.statusCode() == definition.readinessStatus()) {
                    return;
                }
                lastFailure = new IllegalStateException("Readiness returned HTTP " + response.statusCode());
            } catch (IOException expected) {
                // A cold process can refuse connections until ready; retain the cause for timeout diagnostics.
                lastFailure = expected;
            }
            Thread.sleep(Math.min(100, Math.max(1, (deadline - System.nanoTime()) / 1_000_000)));
        }
        throw new IllegalStateException("Readiness timed out for on-demand workload " + definition.id(), lastFailure);
    }

    @Override
    public void close() {
        client.shutdownNow();
    }
}
