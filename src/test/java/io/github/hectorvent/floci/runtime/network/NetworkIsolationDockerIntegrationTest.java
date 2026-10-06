package io.github.hectorvent.floci.runtime.network;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.Capability;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Network;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import io.github.hectorvent.floci.core.common.docker.ContainerBuilder;
import io.github.hectorvent.floci.core.common.docker.ContainerExec;
import io.github.hectorvent.floci.core.common.docker.LocallyBuiltHelperImage;
import io.github.hectorvent.floci.core.common.docker.RetryingTarCopier;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
class NetworkIsolationDockerIntegrationTest {
    @Inject DockerClient docker;
    @Inject ContainerBuilder builder;

    @Test
    void outerBoundaryBlocksDirectIpAndNodeForwardingWhileKeepingLocalPeersReachable() {
        try {
            Assumptions.assumeTrue("linux".equals(docker.infoCmd().exec().getOsType()));
        } catch (Exception e) {
            Assumptions.abort("Docker is unavailable");
        }
        LocallyBuiltHelperImage.ensureBuilt(docker, builder, "floci/network-helper:local", "floci/network-helper:local",
                "/docker/network-helper.Dockerfile");
        String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        String table = "floci_iso_" + suffix;
        String ipv6Prefix = "fd" + suffix.substring(0, 2) + ":" + suffix.substring(2, 6) + ":" + suffix.substring(6, 10);
        String protectedNetwork = docker.createNetworkCmd().withName("floci-isolation-test-" + suffix)
                .withEnableIpv6(true).withIpam(new Network.Ipam()
                        .withConfig(new Network.Ipam.Config().withSubnet(ipv6Prefix + ":1::/64"))).exec().getId();
        String externalNetwork = docker.createNetworkCmd().withName("floci-outside-test-" + suffix)
                .withEnableIpv6(true).withIpam(new Network.Ipam()
                        .withConfig(new Network.Ipam.Config().withSubnet(ipv6Prefix + ":2::/64"))).exec().getId();
        List<String> containers = new ArrayList<>();
        String helper = null;
        try {
            helper = create(containers, "host", false, "sleep", "2147483647");
            String client = create(containers, protectedNetwork, true, "sleep", "2147483647");
            String local = create(containers, protectedNetwork, false, "socat", "TCP6-LISTEN:8080,ipv6only=0,fork,reuseaddr", "EXEC:cat");
            ExposedPort port = ExposedPort.tcp(8080);
            String external = docker.createContainerCmd("floci/network-helper:local")
                    .withExposedPorts(port).withHostConfig(HostConfig.newHostConfig().withNetworkMode(externalNetwork)
                            .withPortBindings(new PortBinding(Ports.Binding.bindPort(0), port)))
                    .withCmd("socat", "TCP6-LISTEN:8080,ipv6only=0,fork,reuseaddr", "EXEC:cat").exec().getId();
            containers.add(external);
            docker.startContainerCmd(external).exec();
            String localAddress = address(local);
            String externalAddress = address(external);
            String host = docker.inspectNetworkCmd().withNetworkId(protectedNetwork).exec()
                    .getIpam().getConfig().stream().map(Network.Ipam.Config::getGateway)
                    .filter(gateway -> !gateway.contains(":")).findFirst().orElseThrow();
            String localIpv6 = ipv6Address(local);
            String externalIpv6 = ipv6Address(external);
            String publishedPort = docker.inspectContainerCmd(external).exec().getNetworkSettings().getPorts()
                    .getBindings().get(port)[0].getHostPortSpec();
            assertEquals(0, connect(client, host, publishedPort).exitCode(), "Host-published destination starts reachable");
            String rules = NetworkIsolationRules.compile(table, List.of("br-" + protectedNetwork.substring(0, 12)));
            RetryingTarCopier.copyBytes(docker, helper, "/tmp", "isolation.nft", rules.getBytes(StandardCharsets.UTF_8), 0600);
            ContainerExec.Result applied = run(helper, "nft", "-f", "/tmp/isolation.nft");
            assertEquals(0, applied.exitCode(), applied.summary());
            assertEquals(0, connect(client, localAddress).exitCode(), "Local simulator remains reachable");
            assertEquals(0, connect(client, localIpv6).exitCode(), "Local IPv6 simulator remains reachable");
            assertNotEquals(0, connect(client, externalAddress).exitCode(), "Direct IP must not bypass isolation");
            assertNotEquals(0, connect(client, host, publishedPort).exitCode(), "Host-published traffic must be denied");
            long dropped = droppedPackets(helper, table);
            assertNotEquals(0, connect(client, externalIpv6).exitCode(), "IPv6 must not bypass isolation");
            assertTrue(droppedPackets(helper, table) > dropped, "Our boundary must observe and drop the IPv6 attempt");
            // A k3s node is privileged and owns its own nftables. Clearing those rules must not
            // change the outer bridge boundary.
            assertEquals(0, run(client, "nft", "flush", "ruleset").exitCode());
            assertNotEquals(0, connect(client, externalAddress).exitCode(), "Node firewall changes must not reopen egress");
            assertEquals(0, connect(client, localAddress).exitCode());
            assertEquals(0, run(helper, "nft", "delete", "table", "inet", table).exitCode());
            assertEquals(0, connect(client, host, publishedPort).exitCode(), "Removing only our policy restores connectivity");
        } finally {
            if (helper != null) {
                run(helper, "nft", "delete", "table", "inet", table);
            }
            for (String id : containers.reversed()) {
                docker.removeContainerCmd(id).withForce(true).exec();
            }
            docker.removeNetworkCmd(protectedNetwork).exec();
            docker.removeNetworkCmd(externalNetwork).exec();
        }
    }

    private String create(List<String> containers, String network, boolean privileged, String... command) {
        String id = docker.createContainerCmd("floci/network-helper:local")
                .withHostConfig(HostConfig.newHostConfig().withNetworkMode(network).withPrivileged(privileged)
                        .withCapAdd(Capability.NET_ADMIN)).withCmd(command).exec().getId();
        containers.add(id);
        docker.startContainerCmd(id).exec();
        return id;
    }

    private String address(String id) {
        return docker.inspectContainerCmd(id).exec().getNetworkSettings().getNetworks().values().iterator().next().getIpAddress();
    }

    private String ipv6Address(String id) {
        return docker.inspectContainerCmd(id).exec().getNetworkSettings().getNetworks().values().iterator().next()
                .getGlobalIPv6Address();
    }

    private long droppedPackets(String helper, String table) {
        String output = run(helper, "nft", "list", "chain", "inet", table, "boundary").stdout();
        return Long.parseLong(output.split("counter packets ")[1].split(" ")[0]);
    }

    private ContainerExec.Result connect(String client, String address) {
        return connect(client, address, "8080");
    }

    private ContainerExec.Result connect(String client, String address, String port) {
        return run(client, "nc", "-z", "-w", "1", address, port);
    }

    private ContainerExec.Result run(String id, String... command) {
        return ContainerExec.run(docker, id, command, 10);
    }
}
