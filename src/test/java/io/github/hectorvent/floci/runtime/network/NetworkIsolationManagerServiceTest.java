package io.github.hectorvent.floci.runtime.network;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.ContainerNetwork;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Network;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.config.FlociCertificateAuthority;
import io.github.hectorvent.floci.core.common.dns.EmbeddedDnsServer;
import io.github.hectorvent.floci.core.common.docker.ContainerBuilder;
import io.github.hectorvent.floci.core.common.docker.ContainerDetector;
import io.github.hectorvent.floci.core.common.docker.ContainerExec;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager;
import io.github.hectorvent.floci.core.common.docker.ContainerLogStreamer;
import io.github.hectorvent.floci.core.common.docker.ContainerSpec;
import io.github.hectorvent.floci.core.common.docker.ContainerStorageHelper;
import io.github.hectorvent.floci.core.common.docker.CurrentContainerNetworkResolver;
import io.github.hectorvent.floci.core.common.docker.DockerHostResolver;
import io.github.hectorvent.floci.core.common.docker.PortAllocator;
import io.github.hectorvent.floci.core.common.docker.RetryingTarCopier;
import io.github.hectorvent.floci.services.lambda.launcher.ImageCacheService;
import jakarta.enterprise.inject.Instance;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.MockMakers;
import org.mockito.MockedStatic;

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class NetworkIsolationManagerServiceTest {
    private final NetworkConfiguration configuration = mock(NetworkConfiguration.class);
    private final EmulatorConfig config = mock(EmulatorConfig.class, RETURNS_DEEP_STUBS);
    private final DockerClient docker = mock(DockerClient.class, RETURNS_DEEP_STUBS);
    private final ContainerDetector detector = mock(ContainerDetector.class);
    private final EmbeddedDnsServer dns = mock(EmbeddedDnsServer.class);

    @Test
    void localDnsBackendMustStillResolveToAnOwnedNetworkAddress() {
        NetworkIsolationManager manager = spy(manager());
        doReturn("isolated").when(manager).networkName();
        doReturn("172.22.0.2").when(manager).flociAddress();
        Network network = mock(Network.class);
        when(docker.inspectNetworkCmd().withNetworkId("isolated").exec()).thenReturn(network);
        when(network.getName()).thenReturn("isolated");
        Map<String, Network.ContainerNetworkConfig> members = new LinkedHashMap<>();
        members.put("removed", new Network.ContainerNetworkConfig());
        members.put("self", new Network.ContainerNetworkConfig());
        when(network.getContainers()).thenReturn(members);
        when(docker.inspectContainerCmd("removed").exec()).thenThrow(new NotFoundException("Container was removed"));
        InspectContainerResponse container = mock(InspectContainerResponse.class, RETURNS_DEEP_STUBS);
        ContainerNetwork endpoint = mock(ContainerNetwork.class);
        when(endpoint.getIpAddress()).thenReturn("172.22.0.2");
        when(docker.inspectContainerCmd("self").exec()).thenReturn(container);
        when(container.getName()).thenReturn("/floci");
        when(container.getNetworkSettings().getNetworks()).thenReturn(Map.of("isolated", endpoint));
        when(configuration.gatewayAddress()).thenReturn(Optional.of("172.22.0.3"));
        when(dns.resolveARecord("simulator.localhost.floci.io", "172.22.0.2")).thenReturn(List.of("172.22.0.2"));
        clearInvocations(docker);
        assertEquals("172.22.0.2", manager.resolveBackend(URI.create("http://simulator.localhost.floci.io:8080")));
        assertEquals("172.22.0.2", manager.resolveBackend(URI.create("http://simulator.localhost.floci.io:8081")));
        verify(docker, times(1)).inspectContainerCmd("self");
        when(dns.resolveARecord("external.test", "172.22.0.2")).thenReturn(List.of("192.0.2.1"));
        assertThrows(IllegalArgumentException.class, () -> manager.resolveBackend(URI.create("http://external.test")));
        when(configuration.gatewayAddress()).thenReturn(Optional.of("172.22.0.2"));
        assertThrows(IllegalArgumentException.class,
                () -> manager.resolveBackend(URI.create("http://simulator.localhost.floci.io:8080")));
    }

    @Test
    void disabledNetworkLeavesContainersAndDockerUntouched() {
        when(configuration.network()).thenReturn(NetworkDefinition.DISABLED);
        when(configuration.configured()).thenReturn(false);
        when(detector.isRunningInContainer()).thenReturn(true);
        NetworkIsolationManager manager = manager();
        ContainerSpec spec = new ContainerSpec("application:1");
        HostConfig host = HostConfig.newHostConfig().withNetworkMode("bridge").withDns("192.0.2.1");
        manager.prepare(spec, host);
        manager.beforeStart("existing");
        manager.created("existing", spec);
        manager.initialize();
        manager.disableExistingPolicy();
        assertFalse(manager.hasPendingPodAdmission("existing"));
        assertEquals(spec.env(), manager.environment(spec));
        assertEquals("bridge", host.getNetworkMode());
        assertArrayEquals(new String[]{"192.0.2.1"}, host.getDns());
        verifyNoInteractions(docker);
    }

    @Test
    void reservesPublicCaVariableAndK3sResolverWithoutChangingOtherVariables() {
        when(configuration.network()).thenReturn(new NetworkDefinition(true, List.of()));
        ContainerSpec spec = new ContainerBuilder(config, mock(DockerHostResolver.class), dns)
                .newContainer("application:1")
                .withEnv(List.of("APP_MODE=test", "FLOCI_CA_BUNDLE=/bad", "K3S_RESOLV_CONF=/bad"))
                .withLabels(Map.of(ContainerStorageHelper.SERVICE_LABEL, "eks")).build();
        assertEquals(List.of("APP_MODE=test", "FLOCI_CA_BUNDLE=" + NetworkIsolationManager.CA_PATH,
                "K3S_RESOLV_CONF=/etc/floci-resolv.conf"), manager().environment(spec));
        verifyNoInteractions(docker);
    }

    @Test
    void rejectsNativeHostAndRootlessDockerBeforeStartingWorkloads() {
        when(configuration.network()).thenReturn(new NetworkDefinition(true, List.of()));
        assertThrows(IllegalStateException.class, () -> manager().initialize());
        verifyNoInteractions(docker);
        when(detector.isRunningInContainer()).thenReturn(true);
        when(docker.infoCmd().exec().getOsType()).thenReturn("linux");
        when(docker.infoCmd().exec().getSecurityOptions()).thenReturn(List.of("name=rootless"));
        assertThrows(IllegalStateException.class, () -> manager().initialize());
        verify(docker, never()).startContainerCmd(anyString());
    }

    @Test
    @SuppressWarnings("unchecked")
    void lifecycleFailsClosedWhenEnforcementCannotBeInstalled() {
        NetworkIsolationManager isolation = mock(NetworkIsolationManager.class);
        Instance<NetworkIsolationManager> provider = mock(Instance.class);
        when(provider.get()).thenReturn(isolation);
        ContainerLifecycleManager lifecycle = new ContainerLifecycleManager(docker, mock(ImageCacheService.class),
                detector, mock(PortAllocator.class), config, provider);
        doThrow(new IllegalStateException("monitor unavailable")).when(isolation).beforeStart("application");
        assertThrows(IllegalStateException.class, () -> lifecycle.startContainer("application"));
        verify(docker, never()).startContainerCmd(anyString());
        doNothing().when(isolation).beforeStart("application");
        lifecycle.startContainer("application");
        InOrder order = inOrder(isolation, docker);
        order.verify(isolation, times(2)).beforeStart("application");
        order.verify(docker).startContainerCmd("application");
    }

    @Test
    void helperContractPublishesOnlyAfterSuccessfulPolicyAndNamesExactTopology() throws Exception {
        NetworkIsolationManager manager = managerWithHelper();
        List<String> events = new ArrayList<>();
        List<byte[]> contracts = new ArrayList<>();
        try (MockedStatic<ContainerExec> execution = mockExec();
             MockedStatic<RetryingTarCopier> copies = mockStatic(RetryingTarCopier.class, withSettings().mockMaker(MockMakers.INLINE))) {
            execution.when(() -> ContainerExec.run(eq(docker), eq("helper"), any(String[].class), eq(15)))
                    .thenAnswer(invocation -> {
                        String[] command = invocation.getArgument(2);
                        events.add("nft".equals(command[0]) ? "apply" : "publish");
                        return new ContainerExec.Result(0, "", "", false);
                    });
            copies.when(() -> RetryingTarCopier.copyBytes(eq(docker), eq("helper"), eq("/etc/floci"),
                    eq(".network-policy.json.tmp"), any(byte[].class), eq(0600)))
                    .thenAnswer(invocation -> {
                        events.add("stage");
                        contracts.add(invocation.getArgument(4));
                        return null;
                    });
            manager.initialize();
            assertEquals(List.of("apply", "stage", "publish"), events);
            JsonNode contract = new ObjectMapper().readTree(contracts.getFirst());
            assertEquals(1, contract.path("version").intValue());
            assertEquals("contract-test/4566", contract.path("owner").textValue());
            assertEquals("floci_iso_contract", contract.path("table").textValue());
            assertEquals("a".repeat(64), contract.path("fingerprint").textValue());
            assertEquals(1, contract.path("bridges").size());
            assertEquals("isolated", contract.at("/bridges/0/name").textValue());
            assertEquals("b".repeat(64), contract.at("/bridges/0/networkId").textValue());
            assertEquals("custom-bridge", contract.at("/bridges/0/bridge").textValue());
        }
    }

    @Test
    void failedPolicyDoesNotPublishAHelperContract() {
        NetworkIsolationManager manager = managerWithHelper();
        try (MockedStatic<ContainerExec> execution = mockExec();
             MockedStatic<RetryingTarCopier> copies = mockStatic(RetryingTarCopier.class, withSettings().mockMaker(MockMakers.INLINE))) {
            execution.when(() -> ContainerExec.run(eq(docker), eq("helper"), any(String[].class), eq(15)))
                    .thenReturn(new ContainerExec.Result(1, "", "incompatible set", false));
            assertThrows(IllegalStateException.class, manager::initialize);
            copies.verify(() -> RetryingTarCopier.copyBytes(eq(docker), eq("helper"), eq("/etc/floci"),
                    eq(".network-policy.json.tmp"), any(byte[].class), anyInt()), never());
        }
    }

    private MockedStatic<ContainerExec> mockExec() {
        return mockStatic(ContainerExec.class, withSettings().mockMaker(MockMakers.INLINE).defaultAnswer(invocation ->
                "run".equals(invocation.getMethod().getName()) ? null : invocation.callRealMethod()));
    }

    @SuppressWarnings("unchecked")
    private NetworkIsolationManager managerWithHelper() {
        when(configuration.network()).thenReturn(new NetworkDefinition(true, List.of()));
        when(configuration.fingerprint()).thenReturn("a".repeat(64));
        when(config.port()).thenReturn(4566);
        when(config.docker().resourceNamespace()).thenReturn(Optional.of("contract-test"));
        when(detector.isRunningInContainer()).thenReturn(true);
        when(docker.infoCmd().exec().getOsType()).thenReturn("linux");
        when(docker.infoCmd().exec().getSecurityOptions()).thenReturn(List.of());
        CurrentContainerNetworkResolver current = mock(CurrentContainerNetworkResolver.class);
        when(current.resolveNetworkName()).thenReturn(Optional.of("isolated"));
        when(current.resolveContainerId()).thenReturn(Optional.of("self"));
        Network network = mock(Network.class);
        when(network.getId()).thenReturn("b".repeat(64));
        when(network.getName()).thenReturn("isolated");
        when(network.getDriver()).thenReturn("bridge");
        when(network.getOptions()).thenReturn(Map.of("com.docker.network.bridge.name", "custom-bridge"));
        when(docker.inspectNetworkCmd().withNetworkId("isolated").exec()).thenReturn(network);
        InspectContainerResponse self = mock(InspectContainerResponse.class, RETURNS_DEEP_STUBS);
        when(self.getHostConfig().getDns()).thenReturn(new String[]{"127.0.0.1"});
        when(self.getNetworkSettings().getNetworks()).thenReturn(Map.of("isolated", new ContainerNetwork()));
        when(docker.inspectContainerCmd("self").exec()).thenReturn(self);
        InspectContainerResponse helper = mock(InspectContainerResponse.class, RETURNS_DEEP_STUBS);
        when(helper.getId()).thenReturn("helper");
        when(helper.getConfig().getLabels()).thenReturn(Map.of("floci.network.owner", "contract-test/4566",
                "floci.network.table", "floci_iso_contract"));
        when(helper.getState().getRunning()).thenReturn(true);
        when(docker.inspectContainerCmd(ContainerStorageHelper.resourceName(config, "network-policy", null, "monitor")).exec())
                .thenReturn(helper);
        when(docker.listContainersCmd().withShowAll(true).exec()).thenReturn(List.of());
        Instance<FlociCertificateAuthority> authority = mock(Instance.class);
        FlociCertificateAuthority certificateAuthority = mock(FlociCertificateAuthority.class, withSettings().mockMaker(MockMakers.INLINE));
        when(authority.get()).thenReturn(certificateAuthority);
        when(certificateAuthority.caPem()).thenReturn("public CA");
        Instance<EmbeddedDnsServer> resolver = mock(Instance.class);
        return new NetworkIsolationManager(configuration, config, docker, mock(ContainerBuilder.class), detector,
                current, authority, mock(ContainerLogStreamer.class), resolver);
    }

    @SuppressWarnings("unchecked")
    private NetworkIsolationManager manager() {
        Instance<FlociCertificateAuthority> authority = mock(Instance.class);
        Instance<EmbeddedDnsServer> resolver = mock(Instance.class);
        when(resolver.get()).thenReturn(dns);
        return new NetworkIsolationManager(configuration, config, docker, mock(ContainerBuilder.class), detector,
                mock(CurrentContainerNetworkResolver.class), authority, mock(ContainerLogStreamer.class), resolver);
    }
}
