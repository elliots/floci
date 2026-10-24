package io.github.hectorvent.floci.runtime.network;

import io.github.hectorvent.floci.config.EmulatorConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Floci's ports that pods of a smolvm-pods node reach at the node address: the AWS endpoint and
 * the listeners that Floci's services publish for their clients, such as brokers and database
 * proxies. Floci's own containers, the nodes' APIs and NodePorts, and ports that a route owns are
 * not in the set; {@link #EXCLUDED} names each with its reason.
 */
@ApplicationScoped
public class FlociPortInventory {

    /** A port or a range of ports, inclusive, and the setting it comes from. */
    public record PortRange(int first, int last, String key) {

        public boolean contains(int port) {
            return first <= port && port <= last;
        }

        /** {@code PORT} or {@code FIRST-LAST}. */
        public String wire() {
            return first == last ? Integer.toString(first) : first + "-" + last;
        }
    }

    /** The settings in the set, by their application.yml key below {@code floci.}. */
    static final List<String> INCLUDED = List.of(
            "port",
            "services.amazonmq.amqp-host-port-base", "services.amazonmq.amqp-host-port-max",
            "services.amazonmq.console-host-port-base", "services.amazonmq.console-host-port-max",
            "services.iot.mqtt.port", "services.iot.mqtt.tls-port",
            "services.elasticache.proxy-base-port", "services.elasticache.proxy-max-port",
            "services.memorydb.proxy-base-port", "services.memorydb.proxy-max-port",
            "services.redshift.proxy-base-port", "services.redshift.proxy-max-port",
            "services.rds.proxy-base-port", "services.rds.proxy-max-port",
            "services.dsql.port",
            "services.neptune.proxy-base-port", "services.neptune.proxy-max-port",
            "services.opensearch.proxy-base-port", "services.opensearch.proxy-max-port",
            "services.mwaa.proxy-base-port", "services.mwaa.proxy-max-port",
            "services.msk.kafka-host-port-base", "services.msk.kafka-host-port-max",
            "services.ecr.registry-base-port", "services.ecr.registry-max-port",
            "services.timestream-influxdb.host-port-base", "services.timestream-influxdb.host-port-max");

    /** The settings that hold a port and are not in the set, and why. */
    static final Map<String, String> EXCLUDED = Map.ofEntries(
            Map.entry("tls.aws-https-port", "routed origins own 443"),
            Map.entry("services.lambda.runtime-api-base-port", "Floci's own Lambda containers"),
            Map.entry("services.lambda.runtime-api-max-port", "Floci's own Lambda containers"),
            Map.entry("services.pipes.kafka-rest-bridge.host-port-base", "Floci's own Pipes containers"),
            Map.entry("services.pipes.kafka-rest-bridge.host-port-max", "Floci's own Pipes containers"),
            Map.entry("services.redshift.default-port", "a database port inside a cluster, not on the Floci host"),
            Map.entry("services.ec2.imds-port", "pods reach no instance metadata"),
            Map.entry("services.ec2.ssh-port-range-start", "EC2 instances, not pods"),
            Map.entry("services.ec2.ssh-port-range-end", "EC2 instances, not pods"),
            Map.entry("services.ec2.app-port-range-start", "EC2 instances, not pods"),
            Map.entry("services.ec2.app-port-range-end", "EC2 instances, not pods"),
            Map.entry("services.ec2.publish-security-group-ports", "a switch, not a port"),
            Map.entry("services.ec2.max-published-ports-per-instance", "a count, not a port"),
            Map.entry("services.ecs.publish-awsvpc-ports-to-host", "a switch, not a port"),
            Map.entry("services.ecs.task-role-credentials.port", "ECS tasks, not pods"),
            Map.entry("services.ui.port", "the web console"),
            Map.entry("services.eks.api-server-base-port", "the k3s containers' Kubernetes APIs"),
            Map.entry("services.eks.api-server-max-port", "the k3s containers' Kubernetes APIs"),
            Map.entry("runtime.node.api-server-base-port", "the node's Kubernetes API"),
            Map.entry("runtime.node.api-server-max-port", "the node's Kubernetes API"),
            Map.entry("runtime.node.node-service-base-port", "the node's service API"),
            Map.entry("runtime.node.node-service-max-port", "the node's service API"),
            Map.entry("runtime.node.node-ports", "the node's own NodePorts"),
            Map.entry("runtime.node.reported-memory-gib", "a size, not a port"),
            Map.entry("runtime.node.reported-cpus", "a count, not a port"),
            Map.entry("services.mwaa.supported-versions", "versions, not a port"),
            Map.entry("services.cloudformation.allow-stub-unsupported-resource-types", "a switch, not a port"),
            Map.entry("runtime.dns-listen", "the nodes' DNS forwarders only, at the Floci host"));

    private final List<PortRange> ranges;

    @Inject
    public FlociPortInventory(EmulatorConfig config) {
        this(ranges(config));
    }

    FlociPortInventory(List<PortRange> ranges) {
        this.ranges = List.copyOf(ranges);
    }

    private static List<PortRange> ranges(EmulatorConfig config) {
        EmulatorConfig.ServicesConfig s = config.services();
        List<PortRange> ranges = new ArrayList<>();
        ranges.add(new PortRange(config.port(), config.port(), "port"));
        ranges.add(new PortRange(s.amazonmq().amqpHostPortBase(), s.amazonmq().amqpHostPortMax(),
                "services.amazonmq.amqp-host-port-base"));
        ranges.add(new PortRange(s.amazonmq().consoleHostPortBase(), s.amazonmq().consoleHostPortMax(),
                "services.amazonmq.console-host-port-base"));
        ranges.add(new PortRange(s.iot().mqtt().port(), s.iot().mqtt().port(), "services.iot.mqtt.port"));
        ranges.add(new PortRange(s.iot().mqtt().tlsPort(), s.iot().mqtt().tlsPort(), "services.iot.mqtt.tls-port"));
        ranges.add(new PortRange(s.elasticache().proxyBasePort(), s.elasticache().proxyMaxPort(),
                "services.elasticache.proxy-base-port"));
        ranges.add(new PortRange(s.memorydb().proxyBasePort(), s.memorydb().proxyMaxPort(),
                "services.memorydb.proxy-base-port"));
        ranges.add(new PortRange(s.redshift().proxyBasePort(), s.redshift().proxyMaxPort(),
                "services.redshift.proxy-base-port"));
        ranges.add(new PortRange(s.rds().proxyBasePort(), s.rds().proxyMaxPort(), "services.rds.proxy-base-port"));
        ranges.add(new PortRange(s.dsql().port(), s.dsql().port(), "services.dsql.port"));
        ranges.add(new PortRange(s.neptune().proxyBasePort(), s.neptune().proxyMaxPort(),
                "services.neptune.proxy-base-port"));
        ranges.add(new PortRange(s.opensearch().proxyBasePort(), s.opensearch().proxyMaxPort(),
                "services.opensearch.proxy-base-port"));
        ranges.add(new PortRange(s.mwaa().proxyBasePort(), s.mwaa().proxyMaxPort(), "services.mwaa.proxy-base-port"));
        ranges.add(new PortRange(s.msk().kafkaHostPortBase(), s.msk().kafkaHostPortMax(),
                "services.msk.kafka-host-port-base"));
        ranges.add(new PortRange(s.ecr().registryBasePort(), s.ecr().registryMaxPort(),
                "services.ecr.registry-base-port"));
        ranges.add(new PortRange(s.timestreamInfluxdb().hostPortBase(), s.timestreamInfluxdb().hostPortMax(),
                "services.timestream-influxdb.host-port-base"));
        return ranges;
    }

    /** Floci's ports, with {@code extraPorts} from the runtime file's {@code network.floci}. */
    public List<PortRange> ports(List<Integer> extraPorts) {
        List<PortRange> all = new ArrayList<>(ranges);
        for (int port : extraPorts) {
            all.add(new PortRange(port, port, "network.floci.extra-ports"));
        }
        return all;
    }

    /** The setting whose port {@code port} is, if one is. */
    public Optional<String> owner(int port) {
        return ranges.stream().filter(range -> range.contains(port)).map(PortRange::key).findFirst();
    }
}
