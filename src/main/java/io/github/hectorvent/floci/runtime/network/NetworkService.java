package io.github.hectorvent.floci.runtime.network;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.ContainerNetwork;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.config.FlociCertificateAuthority;
import io.github.hectorvent.floci.config.NetworkExposureGuard;
import io.github.hectorvent.floci.core.common.docker.ContainerBuilder;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager;
import io.github.hectorvent.floci.core.common.docker.ContainerSpec;
import io.github.hectorvent.floci.core.common.docker.ContainerStorageHelper;
import io.github.hectorvent.floci.core.common.http.HttpReverseProxy;
import io.github.hectorvent.floci.services.acm.CertificateGenerator.GeneratedCertificate;
import io.github.hectorvent.floci.services.acm.model.KeyAlgorithm;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.net.PemTrustOptions;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.interceptor.Interceptor;
import org.jboss.logging.Logger;

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/** Serves simulated HTTP(S) origins through a dedicated local gateway. */
@ApplicationScoped
public class NetworkService {
    private static final Logger LOG = Logger.getLogger(NetworkService.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final NetworkConfiguration configuration;
    private final NetworkIsolationManager isolation;
    private final EmulatorConfig config;
    private final Instance<FlociCertificateAuthority> authority;
    private final ContainerBuilder builder;
    private final ContainerLifecycleManager lifecycle;
    private final DockerClient docker;
    private final Vertx vertx;
    private final List<HttpServer> servers = new ArrayList<>();
    private HttpReverseProxy proxy;
    private String relay;

    @Inject
    public NetworkService(NetworkConfiguration configuration, NetworkIsolationManager isolation,
                                  EmulatorConfig config, Instance<FlociCertificateAuthority> authority,
                                  ContainerBuilder builder,
                                  ContainerLifecycleManager lifecycle, DockerClient docker, Vertx vertx) {
        this.configuration = configuration;
        this.isolation = isolation;
        this.config = config;
        this.authority = authority;
        this.builder = builder;
        this.lifecycle = lifecycle;
        this.docker = docker;
        this.vertx = vertx;
    }

    void start(@Observes @Priority(Interceptor.Priority.APPLICATION + 50) StartupEvent ignored) {
        if (!isolation.active()) {
            isolation.disableExistingPolicy();
            return;
        }
        try {
            isolation.initialize();
            String address = isolation.flociAddress();
            NetworkExposureGuard.requireConsent(address, config.security());
            FlociCertificateAuthority ca = authority.get();
            List<String> names = new ArrayList<>();
            names.add(NetworkPodAdmission.HOST);
            configuration.network().routes().stream().flatMap(route -> route.origins().stream())
                    .map(URI::getHost).distinct().forEach(names::add);
            GeneratedCertificate certificate = ca.issueServerCertificate(NetworkPodAdmission.HOST, names,
                    KeyAlgorithm.RSA_2048, null);
            PemKeyCertOptions keys = new PemKeyCertOptions().setCertValue(Buffer.buffer(certificate.certificatePem()))
                    .setKeyValue(Buffer.buffer(certificate.privateKeyPem()));
            proxy = new HttpReverseProxy(vertx,
                    new PemTrustOptions().addCertValue(Buffer.buffer(ca.caPem())), isolation::resolveBackend);
            NetworkOriginRouter router = new NetworkOriginRouter(configuration.network(), proxy);
            Map<Integer, String> ports = new LinkedHashMap<>();
            for (NetworkDefinition.Route route : configuration.network().routes()) {
                for (URI origin : route.origins()) {
                    if (origin.getPort() == NetworkPodAdmission.PORT) {
                        throw new IllegalArgumentException("Origin port 9443 is reserved for Kubernetes trust admission");
                    }
                    ports.put(origin.getPort(), origin.getScheme());
                }
            }
            StringBuilder command = new StringBuilder();
            for (Map.Entry<Integer, String> port : ports.entrySet()) {
                HttpServer server = vertx.createHttpServer(options("https".equals(port.getValue()), keys))
                        .requestHandler(request -> router.route(request, port.getValue(), port.getKey()));
                servers.add(server);
                server.listen(0, address).toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
                relayCommand(command, port.getKey(), address, server.actualPort());
            }
            HttpServer admission = vertx.createHttpServer(options(true, keys)).requestHandler(request -> {
                if (!NetworkPodAdmission.PATH.equals(request.path()) || !"POST".equals(request.method().name())) {
                    request.response().setStatusCode(404).end();
                    return;
                }
                request.body().onSuccess(body -> {
                    try {
                        request.response().putHeader("Content-Type", "application/json")
                                .end(MAPPER.writeValueAsString(NetworkPodAdmission.review(MAPPER.readTree(body.getBytes()))));
                    } catch (Exception e) {
                        LOG.warnv("Network pod admission rejected an invalid review: {0}", e.getMessage());
                        request.response().setStatusCode(400).end();
                    }
                });
            });
            servers.add(admission);
            admission.listen(0, address).toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            relayCommand(command, NetworkPodAdmission.PORT, address, admission.actualPort());
            command.append("wait");
            String name = ContainerStorageHelper.resourceName(config, "network-gateway", null, "origins");
            lifecycle.removeIfExists(name);
            ContainerSpec spec = builder.newContainer(config.network().securityGroupEnforcement().helperImage())
                    .withName(name).withDockerNetwork(Optional.of(isolation.networkName()))
                    .withEntrypoint(List.of("sh", "-c")).withCmd(List.of(command.toString()))
                    .withLogRotation().build();
            relay = lifecycle.createAndStart(spec).containerId();
            ContainerNetwork endpoint = docker.inspectContainerCmd(relay).exec().getNetworkSettings()
                    .getNetworks().get(isolation.networkName());
            configuration.gatewayAddress(endpoint.getIpAddress());
            LOG.infov("Workload network gateway address={0} isolation={1} routes={2}", endpoint.getIpAddress(),
                    configuration.network().isolated(), configuration.network().routes().size());
        } catch (Exception e) {
            close();
            throw new IllegalStateException("Cannot start workload network gateway", e);
        }
    }

    private static HttpServerOptions options(boolean tls, PemKeyCertOptions keys) {
        HttpServerOptions options = new HttpServerOptions().setHandle100ContinueAutomatically(true)
                .setHttp2ClearTextEnabled(true);
        if (tls) {
            options.setSsl(true).setKeyCertOptions(keys).setUseAlpn(true)
                    .setAlpnVersions(List.of(HttpVersion.HTTP_2, HttpVersion.HTTP_1_1));
        }
        return options;
    }

    private static void relayCommand(StringBuilder command, int port, String address, int backendPort) {
        command.append("socat TCP-LISTEN:").append(port).append(",fork,reuseaddr TCP:")
                .append(address).append(':').append(backendPort).append(" &\n");
    }

    void stop(@Observes ShutdownEvent ignored) {
        close();
    }

    private void close() {
        configuration.gatewayAddress(null);
        if (relay != null) {
            lifecycle.removeIfExists(relay);
            relay = null;
        }
        servers.forEach(HttpServer::close);
        servers.clear();
        if (proxy != null) {
            proxy.close();
        }
    }
}
