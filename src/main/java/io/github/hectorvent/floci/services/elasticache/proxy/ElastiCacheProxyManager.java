package io.github.hectorvent.floci.services.elasticache.proxy;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.config.FlociCertificateAuthority;
import io.github.hectorvent.floci.core.common.AwsPartitions;
import io.github.hectorvent.floci.core.common.dns.NodeDnsServer;
import io.github.hectorvent.floci.services.elasticache.container.ElastiCacheContainerManager;
import io.github.hectorvent.floci.services.elasticache.model.AuthMode;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * Registry of all active ElastiCache auth proxies.
 * One proxy instance per replication group.
 */
@ApplicationScoped
public class ElastiCacheProxyManager {

    private static final Logger LOG = Logger.getLogger(ElastiCacheProxyManager.class);

    private final SigV4Validator sigV4Validator;
    private final FlociCertificateAuthority authority;
    private final List<String> tlsSuffixes;
    private final String hostname;
    private final ElastiCacheContainerManager containers;
    private final NodeDnsServer nodeDns;
    private final ConcurrentHashMap<String, ElastiCacheAuthProxy> proxies = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Set<String>> tlsHostnames = new ConcurrentHashMap<>();

    @Inject
    public ElastiCacheProxyManager(SigV4Validator sigV4Validator, FlociCertificateAuthority authority,
                                   EmulatorConfig config, ElastiCacheContainerManager containers, NodeDnsServer nodeDns) {
        this.sigV4Validator = sigV4Validator;
        this.authority = authority;
        this.tlsSuffixes = tlsSuffixes(config.services().elasticache().tlsSuffixes());
        this.hostname = config.hostname().orElse("localhost");
        this.containers = containers;
        this.nodeDns = nodeDns;
    }

    public ElastiCacheProxyManager(SigV4Validator validator, FlociCertificateAuthority authority,
                                  EmulatorConfig config) {
        this(validator, authority, config, null, null);
    }

    /** For a manager that serves plain text only. */
    public ElastiCacheProxyManager(SigV4Validator sigV4Validator) {
        this.sigV4Validator = sigV4Validator;
        this.authority = null;
        this.tlsSuffixes = List.of();
        this.hostname = "localhost";
        this.containers = null;
        this.nodeDns = null;
    }

    /** The configured TLS domains, with {@code auto} replaced by the ElastiCache domain of every AWS partition. */
    static List<String> tlsSuffixes(List<String> configured) {
        return configured.stream()
                .flatMap(suffix -> "auto".equals(suffix)
                        ? AwsPartitions.all().stream().map(partition -> "cache." + partition.dnsSuffix())
                        : Stream.of(suffix))
                .distinct().toList();
    }

    /**
     * The hostnames a group's proxy answers to besides the one Floci reports: what a client given an address from its
     * own configuration connects to. Takes effect on the next handshake, whether or not the proxy has started.
     */
    public void setTlsHostnames(String groupId, Set<String> names) {
        if (nodeDns != null) {
            nodeDns.serviceNames("elasticache:" + groupId, names);
        }
        if (names.isEmpty()) {
            tlsHostnames.remove(groupId);
        } else {
            tlsHostnames.put(groupId, Set.copyOf(names));
        }
    }

    public void startProxy(String groupId, AuthMode authMode, int proxyPort,
                           String backendHost, int backendPort,
                           ElastiCacheAuthProxy.PasswordValidator passwordValidator) {
        startProxy(groupId, authMode, proxyPort, backendHost, backendPort, passwordValidator, groupId);
    }

    public void startProxy(String groupId, AuthMode authMode, int proxyPort,
                           String backendHost, int backendPort,
                           ElastiCacheAuthProxy.PasswordValidator passwordValidator, String authIdentity) {
        start(groupId, groupId, authMode, proxyPort, backendHost, backendPort, passwordValidator, authIdentity);
    }

    /** Starts the proxy of one node of a cluster-mode group. It serves the TLS hostnames set for the group. */
    public void startNodeProxy(String nodeId, String groupId, AuthMode authMode, int proxyPort,
                               String backendHost, int backendPort,
                               ElastiCacheAuthProxy.PasswordValidator passwordValidator) {
        start(nodeId, groupId, authMode, proxyPort, backendHost, backendPort, passwordValidator, nodeId);
    }

    private void start(String proxyId, String groupId, AuthMode authMode, int proxyPort,
                       String backendHost, int backendPort,
                       ElastiCacheAuthProxy.PasswordValidator passwordValidator, String authIdentity) {
        ElastiCacheAuthProxy proxy = new ElastiCacheAuthProxy(
                authIdentity, authMode, backendHost, backendPort,
                passwordValidator, sigV4Validator, containers == null ? null : containers::connect);
        if (authority != null) {
            proxy.serveTls(new RedisTlsContext(authority, tlsSuffixes, hostname,
                    () -> tlsHostnames.getOrDefault(groupId, Set.of())).context());
        }
        try {
            proxy.start(proxyPort);
            proxies.put(proxyId, proxy);
        } catch (IOException e) {
            throw new RuntimeException("Failed to start proxy for group " + proxyId
                    + " on port " + proxyPort, e);
        }
    }

    public void stopProxy(String groupId) {
        ElastiCacheAuthProxy proxy = proxies.remove(groupId);
        if (proxy != null) {
            proxy.stop();
            LOG.infov("Stopped proxy for group {0}", groupId);
        }
    }

    public void stopAll() {
        proxies.values().forEach(ElastiCacheAuthProxy::stop);
        proxies.clear();
        if (nodeDns != null) {
            tlsHostnames.keySet().forEach(group -> nodeDns.serviceNames("elasticache:" + group, Set.of()));
        }
        tlsHostnames.clear();
        LOG.info("Stopped all ElastiCache proxies");
    }
}
