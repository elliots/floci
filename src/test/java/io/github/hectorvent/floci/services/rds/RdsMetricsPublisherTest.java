package io.github.hectorvent.floci.services.rds;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.services.cloudwatch.metrics.CloudWatchMetricsService;
import io.github.hectorvent.floci.services.cloudwatch.metrics.model.MetricDatum;
import io.github.hectorvent.floci.services.rds.RdsService.InstanceMetricSource;
import io.github.hectorvent.floci.services.rds.container.RdsContainerManager;
import io.github.hectorvent.floci.services.rds.container.RdsContainerManager.ContainerUsage;
import io.github.hectorvent.floci.services.rds.proxy.RdsProxyManager;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RdsMetricsPublisherTest {

    private static final long GIB = 1024L * 1024 * 1024;

    private final RdsService rdsService = mock(RdsService.class);
    private final RdsContainerManager containerManager = mock(RdsContainerManager.class);
    private final RdsProxyManager proxyManager = mock(RdsProxyManager.class);
    private final CloudWatchMetricsService metricsService = mock(CloudWatchMetricsService.class);
    private final RdsMetricsPublisher publisher = new RdsMetricsPublisher(rdsService, containerManager, proxyManager,
            metricsService, mock(EmulatorConfig.class, Answers.RETURNS_DEEP_STUBS));

    @Test
    void anInstancePublishesItsConnectionsUsageAndFreeStorage() {
        when(rdsService.instanceMetricSources()).thenReturn(List.of(new InstanceMetricSource("111111111111",
                "us-west-2", "orders", null, "arn:db:orders", List.of("relay:orders"), 20 * GIB, "volume-orders")));
        when(proxyManager.openConnections("relay:orders")).thenReturn(3);
        when(containerManager.usage("arn:db:orders")).thenReturn(Optional.of(new ContainerUsage(12.5, GIB, 4 * GIB)));
        when(containerManager.volumeSizes()).thenReturn(Map.of("volume-orders", 2 * GIB));

        publisher.publish(Instant.parse("2026-10-25T10:01:01Z"));

        Map<String, Double> values = published("111111111111", "us-west-2");
        assertEquals(Map.of("CPUUtilization", 12.5, "DatabaseConnections", 3.0, "FreeStorageSpace", 18.0 * GIB,
                "FreeableMemory", 3.0 * GIB), values);
    }

    @Test
    void auroraMembersPublishByInstanceAndTheirClusterByCluster() {
        when(rdsService.instanceMetricSources()).thenReturn(List.of(
                new InstanceMetricSource("111111111111", "us-east-1", "writer", "cl", "arn:cluster:cl",
                        List.of("relay:writer", "relay:cl"), 0, null),
                new InstanceMetricSource("111111111111", "us-east-1", "reader", "cl", "arn:cluster:cl",
                        List.of("relay:reader"), 0, null)));
        when(proxyManager.openConnections("relay:writer")).thenReturn(1);
        when(proxyManager.openConnections("relay:cl")).thenReturn(2);
        when(proxyManager.openConnections("relay:reader")).thenReturn(4);
        when(containerManager.usage("arn:cluster:cl")).thenReturn(Optional.empty());

        publisher.publish(Instant.parse("2026-10-25T10:01:01Z"));

        ArgumentCaptor<List<MetricDatum>> datums = ArgumentCaptor.captor();
        verify(metricsService, times(3)).putMetricDataForAccount(eq("111111111111"),
                eq("AWS/RDS"), datums.capture(), eq("us-east-1"));
        List<List<MetricDatum>> all = datums.getAllValues();
        assertEquals("DBClusterIdentifier", all.get(2).getFirst().getDimensions().getFirst().name());
        assertEquals(List.of(3.0, 4.0, 7.0), all.stream().map(list -> list.getFirst().getValue()).toList());
        assertEquals(1, all.get(2).size());
        verify(containerManager, never()).volumeSizes();
    }

    private Map<String, Double> published(String accountId, String region) {
        ArgumentCaptor<List<MetricDatum>> datums = ArgumentCaptor.captor();
        verify(metricsService).putMetricDataForAccount(eq(accountId), eq("AWS/RDS"), datums.capture(), eq(region));
        Map<String, Double> values = new TreeMap<>();
        for (MetricDatum datum : datums.getValue()) {
            assertEquals(Instant.parse("2026-10-25T10:00:00Z").getEpochSecond(), datum.getTimestamp());
            values.put(datum.getMetricName(), datum.getValue());
        }
        verify(metricsService, never()).putMetricDataForAccount(any(), eq("AWS/RDS"), any(), eq("eu-west-1"));
        return values;
    }
}
