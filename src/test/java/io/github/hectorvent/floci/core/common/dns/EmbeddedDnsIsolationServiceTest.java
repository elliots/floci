package io.github.hectorvent.floci.core.common.dns;

import io.github.hectorvent.floci.runtime.network.NetworkConfiguration;
import io.github.hectorvent.floci.runtime.network.NetworkDefinition;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class EmbeddedDnsIsolationServiceTest {
    @Test
    void unknownNamesNeverForwardAndConfiguredOriginsUseTheGatewayAddress() {
        NetworkConfiguration config = mock(NetworkConfiguration.class);
        when(config.network()).thenReturn(new NetworkDefinition(true, List.of(
                new NetworkDefinition.Route("payments", List.of(URI.create("https://api.vendor.test:443"),
                        URI.create("https://uploads.vendor.test:443")), URI.create("http://mock:8080"), false))));
        when(config.gatewayAddress()).thenReturn(Optional.of("172.20.0.3"));
        EmbeddedDnsServer dns = new EmbeddedDnsServer(List.of(), List.of(), config);
        for (String hostname : List.of("api.vendor.test", "uploads.vendor.test")) {
            EmbeddedDnsServer.QueryPlan plan = dns.planQuery(hostname, "172.20.0.5", "172.20.0.2", 1);
            assertEquals(List.of("172.20.0.3"), plan.answer().orElseThrow().addresses());
            assertTrue(plan.ruleTargets().isEmpty());
            assertTrue(dns.planQuery(hostname, "172.20.0.5", "172.20.0.2", 28).answer().orElseThrow().isEmpty());
        }
        EmbeddedDnsServer.QueryPlan denied = dns.planQuery("unknown.vendor.test", "172.20.0.5", "172.20.0.2", 1);
        assertFalse(denied.answer().orElseThrow().nameExists());
        assertTrue(denied.ruleTargets().isEmpty());
        assertEquals(List.of("172.20.0.2"), dns.planQuery("s3.localhost.floci.io", "172.20.0.5", "172.20.0.2", 1)
                .answer().orElseThrow().addresses());
        when(config.gatewayAddress()).thenReturn(Optional.empty());
        assertFalse(dns.planQuery("api.vendor.test", "172.20.0.5", "172.20.0.2", 1).answer().orElseThrow().nameExists());
    }

    @Test
    void disabledIsolationKeepsPublicResolution() {
        NetworkConfiguration config = mock(NetworkConfiguration.class);
        when(config.network()).thenReturn(NetworkDefinition.DISABLED);
        when(config.gatewayAddress()).thenReturn(Optional.empty());
        EmbeddedDnsServer dns = new EmbeddedDnsServer(List.of(), List.of(), config);
        assertEquals(EmbeddedDnsServer.QueryPlan.UPSTREAM, dns.planQuery("example.test", "172.20.0.5", "172.20.0.2", 1));
    }
}
