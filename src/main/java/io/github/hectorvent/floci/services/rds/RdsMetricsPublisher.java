package io.github.hectorvent.floci.services.rds;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.services.cloudwatch.metrics.CloudWatchMetricsService;
import io.github.hectorvent.floci.services.cloudwatch.metrics.model.Dimension;
import io.github.hectorvent.floci.services.cloudwatch.metrics.model.MetricDatum;
import io.github.hectorvent.floci.services.rds.RdsService.InstanceMetricSource;
import io.github.hectorvent.floci.services.rds.container.RdsContainerManager;
import io.github.hectorvent.floci.services.rds.container.RdsContainerManager.ContainerUsage;
import io.github.hectorvent.floci.services.rds.proxy.RdsProxyManager;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Sends the AWS/RDS metrics of every available DB instance to CloudWatch once a minute, by
 * DBInstanceIdentifier, and those of each Aurora cluster by DBClusterIdentifier:
 * DatabaseConnections, from the connections its endpoints' proxies hold; CPUUtilization and
 * FreeableMemory, from one docker stats sample of its database; and FreeStorageSpace, its
 * AllocatedStorage less what its Docker volume holds. Nothing it reads wakes a sleeping database
 * on a smolvm-pods node, which reports no CPU. Like the AWS/SQS metrics, a tick stamps its samples
 * with the minute that ended, and drops the AWS/RDS datapoints older than 15 days once an hour.
 */
@ApplicationScoped
public class RdsMetricsPublisher {

    static final String NAMESPACE = "AWS/RDS";
    private static final Logger LOG = Logger.getLogger(RdsMetricsPublisher.class);
    private static final long MINUTE_MILLIS = 60_000;
    private static final long TICK_OFFSET_MILLIS = 1_000;
    private static final Duration RETENTION = Duration.ofDays(15);

    private final RdsService rdsService;
    private final RdsContainerManager containerManager;
    private final RdsProxyManager proxyManager;
    private final CloudWatchMetricsService metricsService;
    private final boolean enabled;
    private final ScheduledExecutorService scheduler;

    @Inject
    public RdsMetricsPublisher(RdsService rdsService, RdsContainerManager containerManager,
                               RdsProxyManager proxyManager, CloudWatchMetricsService metricsService,
                               EmulatorConfig config) {
        this.rdsService = rdsService;
        this.containerManager = containerManager;
        this.proxyManager = proxyManager;
        this.metricsService = metricsService;
        this.enabled = config.services().rds().enabled() && !config.services().rds().mock()
                && config.services().cloudwatchmetrics().enabled();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "rds-metrics-publisher");
            t.setDaemon(true);
            return t;
        });
    }

    void onStart(@Observes StartupEvent ignored) {
        if (!enabled) {
            return;
        }
        long untilNextMinute = MINUTE_MILLIS - System.currentTimeMillis() % MINUTE_MILLIS;
        scheduler.scheduleAtFixedRate(() -> publish(Instant.now()), untilNextMinute + TICK_OFFSET_MILLIS,
                MINUTE_MILLIS, TimeUnit.MILLISECONDS);
    }

    /** Before EmulatorLifecycle's default-priority storage shutdown, letting a running tick finish its writes. */
    void onStop(@Observes @Priority(1000) ShutdownEvent ignored) {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(15, TimeUnit.SECONDS)) {
                LOG.warn("The AWS/RDS metrics publisher did not finish its tick within 15 seconds");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warnv(e, "Interrupted while stopping the AWS/RDS metrics publisher");
        }
    }

    /**
     * Nothing may escape: the scheduler stops for good after a tick throws. One instance's failed
     * write drops only its samples for the minute.
     */
    void publish(Instant now) {
        List<InstanceMetricSource> sources;
        try {
            sources = rdsService.instanceMetricSources();
        } catch (RuntimeException e) {
            LOG.warnv(e, "Collecting the AWS/RDS metrics failed; this minute's samples are dropped");
            return;
        }
        long minute = now.minusSeconds(30).truncatedTo(ChronoUnit.MINUTES).getEpochSecond();
        Map<String, Optional<ContainerUsage>> usages = new HashMap<>();
        Map<String, Long> volumes = sources.stream().anyMatch(source -> source.volumeName() != null)
                ? containerManager.volumeSizes() : Map.of();
        Map<String, ClusterTotal> clusters = new LinkedHashMap<>();
        for (InstanceMetricSource source : sources) {
            Optional<ContainerUsage> usage = usages.computeIfAbsent(source.runtimeId(), containerManager::usage);
            int connections = source.relayKeys().stream().mapToInt(proxyManager::openConnections).sum();
            Dimension instance = new Dimension("DBInstanceIdentifier", source.instanceId());
            List<MetricDatum> datums = datums(instance, connections, usage, minute);
            Long used = source.volumeName() == null ? null : volumes.get(source.volumeName());
            if (used != null && source.storageBytes() > 0) {
                datums.add(MetricDatum.of("FreeStorageSpace", "Bytes",
                        Math.max(0, source.storageBytes() - used), List.of(instance), minute));
            }
            put(source.accountId(), source.region(), source.instanceId(), datums);
            if (source.clusterId() != null) {
                clusters.computeIfAbsent(source.accountId() + "/" + source.region() + "/" + source.clusterId(),
                        key -> new ClusterTotal(source, usage.orElse(null))).connections += connections;
            }
        }
        for (ClusterTotal cluster : clusters.values()) {
            InstanceMetricSource source = cluster.source;
            put(source.accountId(), source.region(), source.clusterId(),
                    datums(new Dimension("DBClusterIdentifier", source.clusterId()), cluster.connections,
                            Optional.ofNullable(cluster.usage), minute));
        }
        if (minute % 3600 == 0) {
            try {
                metricsService.deleteMetricDataBefore(NAMESPACE, Instant.ofEpochSecond(minute).minus(RETENTION));
            } catch (RuntimeException e) {
                LOG.warnv(e, "Dropping AWS/RDS datapoints older than {0} failed; the next hour tries again", RETENTION);
            }
        }
    }

    private static List<MetricDatum> datums(Dimension dimension, int connections, Optional<ContainerUsage> usage,
                                            long minute) {
        List<Dimension> dimensions = List.of(dimension);
        List<MetricDatum> datums = new ArrayList<>();
        datums.add(MetricDatum.of("DatabaseConnections", "Count", connections, dimensions, minute));
        usage.ifPresent(sample -> {
            datums.add(MetricDatum.of("CPUUtilization", "Percent", sample.cpuPercent(), dimensions, minute));
            if (sample.memoryLimitBytes() > 0) {
                datums.add(MetricDatum.of("FreeableMemory", "Bytes",
                        Math.max(0, sample.memoryLimitBytes() - sample.memoryUsedBytes()), dimensions, minute));
            }
        });
        return datums;
    }

    private void put(String accountId, String region, String name, List<MetricDatum> datums) {
        try {
            metricsService.putMetricDataForAccount(accountId, NAMESPACE, datums, region);
        } catch (RuntimeException e) {
            LOG.warnv(e, "Publishing the AWS/RDS metrics of {0} in account {1}, {2} failed; its samples for this "
                    + "minute are dropped", name, accountId, region);
        }
    }

    /** An Aurora cluster's connections over its members, and the usage of the database they share. */
    private static final class ClusterTotal {

        private final InstanceMetricSource source;
        private final ContainerUsage usage;
        private int connections;

        private ClusterTotal(InstanceMetricSource source, ContainerUsage usage) {
            this.source = source;
            this.usage = usage;
        }
    }
}
