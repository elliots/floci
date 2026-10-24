package io.github.hectorvent.floci.services.eks;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.core.common.docker.ContainerLogStreamer;
import io.github.hectorvent.floci.runtime.flocinode.NodeKubeApi;
import io.github.hectorvent.floci.runtime.flocinode.NodeKubeApi.PodContainer;
import io.github.hectorvent.floci.runtime.flocinode.NodeRecord;
import io.github.hectorvent.floci.services.eks.model.Cluster;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class EksContainerInsightsTest {

    private static final NodeRecord NODE = new NodeRecord("floci-node-0123456789abcdef", 6501, 6601, "none",
            "a".repeat(64), false, List.of(), "123456789012", "us-east-1", "node-1", "10.100.0.0/16",
            "10.42.0.0/16", Map.of(), List.of(EksSmolvmSeedContributor.NAME));
    private static final PodContainer WEB = new PodContainer("shop", "web-7d4b9c", "uid-1", "node-1", "web",
            "abc123", "nginx:1.27");
    private static final PodContainer WORKER = new PodContainer("shop", "worker-0", "uid-2", "node-1", "worker",
            "def456", "busybox:1.36");
    private static final String GROUP = "/aws/containerinsights/orders/application";

    private final NodeKubeApi kubeApi = mock(NodeKubeApi.class);
    private final ContainerLogStreamer writer = mock(ContainerLogStreamer.class);
    private final EksAddonService addons = mock(EksAddonService.class);
    private final EksContainerInsights insights = new EksContainerInsights(kubeApi, writer, addons);
    private final Cluster cluster = cluster();

    @Test
    void shipsEachContainersLogAsTheAddOnsFluentBitDoes() throws Exception {
        when(addons.isInstalled(cluster, "amazon-cloudwatch-observability")).thenReturn(true);
        when(kubeApi.runningContainers(NODE)).thenReturn(List.of(WEB));
        when(kubeApi.followPodLog(NODE, WEB, true)).thenReturn(new ByteArrayInputStream(
                "2026-10-25T01:00:00.123456789Z GET / 200\n".getBytes(StandardCharsets.UTF_8)));

        insights.reconcile("key", cluster, "123456789012", "us-east-1", NODE);

        String stream = "node-1-application.var.log.containers.web-7d4b9c_shop_web-abc123.log";
        verify(writer, timeout(5_000)).streamToCloudWatchLogsForAccount(eq("123456789012"), eq(GROUP), eq(stream),
                eq("us-east-1"), argThat(message -> {
                    JsonNode event = read(message);
                    return event.path("log").asText().equals("GET / 200")
                            && event.path("time").asText().equals("2026-10-25T01:00:00.123456789Z")
                            && event.path("kubernetes").path("pod_name").asText().equals("web-7d4b9c")
                            && event.path("kubernetes").path("docker_id").asText().equals("abc123")
                            && event.path("kubernetes").path("container_image").asText().equals("nginx:1.27");
                }));
        verify(writer).ensureLogGroupAndStreamForAccount("123456789012", GROUP, stream, "us-east-1");
    }

    @Test
    void aContainerFoundLaterShipsItsWholeLogAndAnEndedOneStops() throws Exception {
        when(addons.isInstalled(cluster, "amazon-cloudwatch-observability")).thenReturn(true);
        PipedOutputStream webLog = new PipedOutputStream();
        PipedInputStream webBody = new PipedInputStream(webLog);
        when(kubeApi.followPodLog(NODE, WEB, true)).thenReturn(webBody);
        when(kubeApi.followPodLog(NODE, WORKER, false)).thenReturn(new ByteArrayInputStream(new byte[0]));
        when(kubeApi.runningContainers(NODE)).thenReturn(List.of(WEB));
        insights.reconcile("key", cluster, "123456789012", "us-east-1", NODE);
        verify(kubeApi, timeout(5_000)).followPodLog(NODE, WEB, true);

        when(kubeApi.runningContainers(NODE)).thenReturn(List.of(WORKER));
        insights.reconcile("key", cluster, "123456789012", "us-east-1", NODE);

        verify(kubeApi, timeout(5_000)).followPodLog(NODE, WORKER, false);
        assertEquals(-1, readClosed(webBody));
    }

    @Test
    void withoutTheAddOnNothingShips() {
        insights.reconcile("key", cluster, "123456789012", "us-east-1", NODE);

        verify(kubeApi, never()).runningContainers(any());
        verify(kubeApi, never()).followPodLog(any(), any(), anyBoolean());
    }

    private static int readClosed(PipedInputStream body) {
        try {
            return body.read();
        } catch (IOException closed) {
            // A closed pipe is what closing the follow does to the pod log's stream.
            return -1;
        }
    }

    private static JsonNode read(String message) {
        try {
            return new ObjectMapper().readTree(message);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static Cluster cluster() {
        Cluster cluster = new Cluster();
        cluster.setName("orders");
        return cluster;
    }
}
