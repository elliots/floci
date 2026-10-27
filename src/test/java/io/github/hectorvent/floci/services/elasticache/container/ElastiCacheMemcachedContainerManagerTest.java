package io.github.hectorvent.floci.services.elasticache.container;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.core.common.RegionResolver;
import io.github.hectorvent.floci.core.common.docker.ContainerBuilder;
import io.github.hectorvent.floci.core.common.docker.ContainerDetector;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager;
import io.github.hectorvent.floci.core.common.docker.ContainerLogStreamer;
import io.github.hectorvent.floci.core.common.docker.ContainerSpec;
import io.github.hectorvent.floci.runtime.node.NodeTcpRelay;
import io.github.hectorvent.floci.runtime.node.NodeTunnel;
import io.github.hectorvent.floci.services.elasticache.ElastiCachePortReservations;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ElastiCacheMemcachedContainerManagerTest {

    private static final Logger LOG = Logger.getLogger(ElastiCacheMemcachedContainerManagerTest.class);

    @Test
    void exhaustedSharedPoolRemovesContainerWithoutReleasingAnotherCachesPort() {
        NodeFixture f = nodeFixture(6380, 6380);
        assertTrue(f.ports().claim(6380));
        AwsException failure = assertThrows(AwsException.class,
                () -> f.manager().tryStart("cache", "memcached:1.6", "us-east-1"));
        assertEquals("InsufficientCacheClusterCapacity", failure.getErrorCode());
        verify(f.lifecycle()).stopAndRemove(eq("node-container"), any());
        assertFalse(f.ports().claim(6380));
    }

    @Test
    void failedNodeStartupSkipsOccupiedPortsAndReleasesItsRelay() throws IOException {
        try (ServerSocket occupied = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
            int base = occupied.getLocalPort();
            NodeFixture f = nodeFixture(base, Math.min(base + 10, 65535));
            AtomicInteger boundPort = new AtomicInteger();
            when(f.backend().relay(eq(f.target()), eq("10.42.0.9"), eq(11211), any(Integer.class)))
                    .thenAnswer(call -> {
                        NodeTcpRelay relay = new NodeTcpRelay(mock(NodeTunnel.class), null, "10.42.0.9",
                                11211, call.getArgument(3));
                        boundPort.set(relay.port());
                        return relay;
                    });
            when(f.logs().attach(any(), any(), any(), any(), any()))
                    .thenThrow(new IllegalStateException("log startup failed"));
            IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> f.manager().start("cache", "memcached:1.6", "us-east-1"));
            assertEquals("log startup failed", failure.getMessage(), () -> failure.toString());
            assertTrue(boundPort.get() > base, "a busy host port must be skipped");
            assertTrue(f.ports().claim(base), "failed bind releases its reservation");
            assertTrue(f.ports().claim(boundPort.get()), "failed startup releases its reservation");
            assertThrows(IOException.class, () -> new Socket("127.0.0.1", boundPort.get()));
            verify(f.lifecycle()).stopAndRemove(eq("node-container"), any());
        }
    }

    private record NodeFixture(ElastiCacheMemcachedContainerManager manager, ContainerLifecycleManager lifecycle,
                               ContainerLogStreamer logs, ElastiCacheSmolvmBackend backend,
                               ElastiCacheSmolvmBackend.Target target, ElastiCachePortReservations ports) {}

    private static NodeFixture nodeFixture(int base, int max) {
        ContainerLifecycleManager lifecycle = mock(ContainerLifecycleManager.class);
        when(lifecycle.createAndStart(any())).thenReturn(new ContainerLifecycleManager.ContainerInfo("node-container", Map.of()));
        when(lifecycle.resolveContainerNetworkIp("node-container", null)).thenReturn("10.42.0.9");
        ContainerLogStreamer logs = mock(ContainerLogStreamer.class);
        ElastiCacheSmolvmBackend.Target target = new ElastiCacheSmolvmBackend.Target(null, null, lifecycle, logs);
        ElastiCacheSmolvmBackend backend = mock(ElastiCacheSmolvmBackend.class);
        when(backend.enabled()).thenReturn(true);
        when(backend.target("000000000000", "us-east-1")).thenReturn(target);
        ContainerBuilder builder = mock(ContainerBuilder.class);
        ContainerBuilder.Builder spec = mock(ContainerBuilder.Builder.class, RETURNS_SELF);
        when(builder.newContainer(anyString())).thenReturn(spec);
        when(spec.build()).thenReturn(new ContainerSpec("memcached:1.6"));
        EmulatorConfig config = mock(EmulatorConfig.class);
        EmulatorConfig.ServicesConfig services = mock(EmulatorConfig.ServicesConfig.class);
        EmulatorConfig.ElastiCacheServiceConfig cache = mock(EmulatorConfig.ElastiCacheServiceConfig.class);
        when(config.services()).thenReturn(services);
        when(services.elasticache()).thenReturn(cache);
        when(cache.proxyBasePort()).thenReturn(base);
        when(cache.proxyMaxPort()).thenReturn(max);
        RegionResolver region = mock(RegionResolver.class);
        when(region.getAccountId()).thenReturn("000000000000");
        ElastiCachePortReservations ports = new ElastiCachePortReservations();
        ElastiCacheMemcachedContainerManager manager = new ElastiCacheMemcachedContainerManager(builder,
                mock(ContainerLifecycleManager.class), mock(ContainerLogStreamer.class), mock(ContainerDetector.class),
                config, region, backend, ports);
        return new NodeFixture(manager, lifecycle, logs, backend, target, ports);
    }

    @Test
    void nodeBackendUsesNodeImageAndClosesRelayOnStop() throws IOException, InterruptedException {
        try (ServerSocket server = new ServerSocket(0)) {
            Thread acceptor = Thread.ofVirtual().start(() -> {
                try (Socket socket = server.accept()) {
                    socket.getInputStream().read(new byte[1024]);
                    socket.getOutputStream().write("VERSION 1.6.0\r\n".getBytes(StandardCharsets.UTF_8));
                } catch (IOException e) {
                    LOG.debugv(e, "Memcached readiness server closed during teardown");
                }
            });
            ContainerLifecycleManager nodeLifecycle = mock(ContainerLifecycleManager.class);
            when(nodeLifecycle.createAndStart(any())).thenReturn(new ContainerLifecycleManager.ContainerInfo(
                    "node-container", Map.of()));
            when(nodeLifecycle.resolveContainerNetworkIp("node-container", null)).thenReturn("10.42.0.9");
            ContainerLogStreamer nodeLogs = mock(ContainerLogStreamer.class);
            ElastiCacheSmolvmBackend.Target target = new ElastiCacheSmolvmBackend.Target(
                    null, null, nodeLifecycle, nodeLogs);
            NodeTunnel tunnel = mock(NodeTunnel.class);
            when(tunnel.open(any(), anyString(), eq(11211), any(), any()))
                    .thenAnswer(call -> new Socket("127.0.0.1", server.getLocalPort()));
            NodeTcpRelay relay = new NodeTcpRelay(tunnel, null, "10.42.0.9", 11211);
            ElastiCacheSmolvmBackend backend = mock(ElastiCacheSmolvmBackend.class);
            when(backend.enabled()).thenReturn(true);
            when(backend.target("000000000000", "us-east-1")).thenReturn(target);
            when(backend.target()).thenReturn(target);
            when(backend.relay(target, "10.42.0.9", 11211, relay.port())).thenReturn(relay);
            ContainerBuilder builder = mock(ContainerBuilder.class);
            ContainerBuilder.Builder specBuilder = mock(ContainerBuilder.Builder.class, RETURNS_SELF);
            when(builder.newContainer("memcached:1.6")).thenReturn(specBuilder);
            when(specBuilder.build()).thenReturn(new ContainerSpec("memcached:1.6"));
            RegionResolver region = mock(RegionResolver.class);
            when(region.getAccountId()).thenReturn("000000000000");
            EmulatorConfig config = mock(EmulatorConfig.class);
            EmulatorConfig.ServicesConfig services = mock(EmulatorConfig.ServicesConfig.class);
            EmulatorConfig.ElastiCacheServiceConfig elasticache = mock(EmulatorConfig.ElastiCacheServiceConfig.class);
            when(config.services()).thenReturn(services);
            when(services.elasticache()).thenReturn(elasticache);
            when(elasticache.proxyBasePort()).thenReturn(relay.port() - 1);
            when(elasticache.proxyMaxPort()).thenReturn(relay.port());
            ElastiCachePortReservations ports = new ElastiCachePortReservations();
            assertTrue(ports.claim(relay.port() - 1));
            ElastiCacheMemcachedContainerManager manager = new ElastiCacheMemcachedContainerManager(
                    builder, mock(ContainerLifecycleManager.class), mock(ContainerLogStreamer.class),
                    mock(ContainerDetector.class), config, region, backend, ports);

            ElastiCacheContainerHandle handle = manager.start("cache", "memcached:1.6", "us-east-1");
            assertEquals(relay.port(), handle.getPort());
            verify(backend).ensureImage(target, "memcached:1.6");
            verify(specBuilder).withExposedPort(11211);
            manager.stop(handle);
            assertThrows(IOException.class, () -> new Socket("127.0.0.1", relay.port()));
            assertTrue(ports.claim(relay.port()), "stopping the relay releases its port");
            assertFalse(ports.claim(relay.port() - 1), "another cache's reservation stays held");
            verify(nodeLifecycle).stopAndRemove(eq("node-container"), any());
            manager.stop(handle);
            assertFalse(ports.claim(relay.port()), "repeated cleanup must not release a reused port");
            acceptor.join(1_000);
            assertFalse(acceptor.isAlive());
        }
    }

    @Test
    void startLabelsContainerWithResourceIdentity() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            Thread acceptor = new Thread(() -> {
                try (Socket socket = serverSocket.accept()) {
                    socket.getInputStream().read(new byte[1024]);
                    socket.getOutputStream().write("VERSION 1.6.0\r\n".getBytes(StandardCharsets.UTF_8));
                    socket.getOutputStream().flush();
                } catch (IOException e) {
                    LOG.debugv(e, "Acceptor socket closed during test teardown");
                }
            });
            acceptor.setDaemon(true);
            acceptor.start();

            ContainerLifecycleManager lifecycleManager = mock(ContainerLifecycleManager.class);
            when(lifecycleManager.createAndStart(any())).thenReturn(new ContainerLifecycleManager.ContainerInfo(
                    "container-id", Map.of(11211,
                            new ContainerLifecycleManager.EndpointInfo(
                                    "127.0.0.1", serverSocket.getLocalPort()))));

            ContainerBuilder containerBuilder = mock(ContainerBuilder.class);
            ContainerBuilder.Builder builder = mock(ContainerBuilder.Builder.class, RETURNS_SELF);
            when(containerBuilder.newContainer(anyString())).thenReturn(builder);
            when(builder.build()).thenReturn(new ContainerSpec("test-image"));

            EmulatorConfig config = mock(EmulatorConfig.class);
            EmulatorConfig.ServicesConfig services = mock(EmulatorConfig.ServicesConfig.class);
            EmulatorConfig.ElastiCacheServiceConfig elasticache = mock(EmulatorConfig.ElastiCacheServiceConfig.class);
            when(config.services()).thenReturn(services);
            when(services.elasticache()).thenReturn(elasticache);
            when(elasticache.dockerNetwork()).thenReturn(Optional.empty());

            RegionResolver regionResolver = mock(RegionResolver.class);
            when(regionResolver.getAccountId()).thenReturn("000000000000");
            when(regionResolver.getDefaultRegion()).thenReturn("us-east-1");

            ElastiCacheMemcachedContainerManager manager = new ElastiCacheMemcachedContainerManager(
                    containerBuilder, lifecycleManager, mock(ContainerLogStreamer.class),
                    mock(ContainerDetector.class), config, regionResolver);

            manager.start("my-cluster", "memcached:1.6", "eu-west-1");

            verify(builder).withLabels(Map.of(
                    "io.floci", "aws",
                    "io.floci.service", "elasticache",
                    "io.floci.resource-id", "my-cluster",
                    "io.floci.account", "000000000000",
                    "io.floci.region", "eu-west-1"));
        }
    }

    @Test
    void tryStartRethrowsWhenDaemonBecomesUnreachableAfterContainerWasCreated() {
        ContainerLifecycleManager lifecycleManager = mock(ContainerLifecycleManager.class);
        when(lifecycleManager.createAndStart(any())).thenReturn(new ContainerLifecycleManager.ContainerInfo(
                "container-id", Map.of(11211,
                        new ContainerLifecycleManager.EndpointInfo("127.0.0.1", 11211))));
        when(lifecycleManager.getDockerClient()).thenThrow(new IllegalStateException("no daemon"));
        ContainerLogStreamer logStreamer = mock(ContainerLogStreamer.class);
        when(logStreamer.attach(any(), any(), any(), any(), any()))
                .thenThrow(new IllegalStateException("log attach failed after daemon went away"));

        ContainerBuilder containerBuilder = mock(ContainerBuilder.class);
        ContainerBuilder.Builder builder = mock(ContainerBuilder.Builder.class, RETURNS_SELF);
        when(containerBuilder.newContainer(anyString())).thenReturn(builder);
        when(builder.build()).thenReturn(new ContainerSpec("test-image"));

        EmulatorConfig config = mock(EmulatorConfig.class);
        EmulatorConfig.ServicesConfig services = mock(EmulatorConfig.ServicesConfig.class);
        EmulatorConfig.ElastiCacheServiceConfig elasticache = mock(EmulatorConfig.ElastiCacheServiceConfig.class);
        when(config.services()).thenReturn(services);
        when(services.elasticache()).thenReturn(elasticache);
        when(elasticache.dockerNetwork()).thenReturn(Optional.empty());

        ElastiCacheMemcachedContainerManager manager = new ElastiCacheMemcachedContainerManager(
                containerBuilder, lifecycleManager, logStreamer, mock(ContainerDetector.class),
                config, mock(RegionResolver.class));

        assertThrows(IllegalStateException.class, () -> manager.tryStart("my-cluster", "memcached:1.6", "us-east-1"));

        verify(lifecycleManager).stopAndRemove(eq("container-id"), any());
    }
}
