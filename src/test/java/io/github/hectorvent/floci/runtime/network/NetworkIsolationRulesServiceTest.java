package io.github.hectorvent.floci.runtime.network;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class NetworkIsolationRulesServiceTest {
    @Test
    void rebuildsOnlyOwnedChainsAndKeepsNativeTupleSets() {
        String rules = NetworkIsolationRules.compile("floci_iso_test", List.of("br-123456789abc"));
        assertFalse(rules.contains("flush table"));
        assertFalse(rules.contains("flush set"));
        assertFalse(rules.contains("delete set"));
        for (String transport : List.of("tcp", "udp")) {
            assertTrue(rules.contains("add set inet floci_iso_test native_" + transport
                    + " { type ipv4_addr . ipv4_addr . inet_service; }"));
            assertTrue(rules.contains("boundary ip saddr . ip daddr . " + transport + " dport @native_" + transport + " accept"));
        }
        assertTrue(rules.contains("hook forward priority -200; policy accept;"));
        assertTrue(rules.contains("hook input priority -200; policy accept;"));
        assertTrue(rules.endsWith("add rule inet floci_iso_test boundary counter drop\n"));
        assertFalse(rules.contains("ip6 daddr"));
    }

    @Test
    void onlyExplicitAddressProtocolAndPortsGainAnException() {
        String rules = NetworkIsolationRules.compile("floci_iso_test", List.of("br-123456789abc"), List.of(
                new NetworkDefinition.Egress("203.0.113.17/32", "tcp", "443"),
                new NetworkDefinition.Egress("198.51.100.0/24", "udp", "4000-4001")));
        assertTrue(rules.contains("boundary ip daddr 203.0.113.17/32 tcp dport 443 accept\n"));
        assertTrue(rules.contains("boundary ip daddr 198.51.100.0/24 udp dport 4000-4001 accept\n"));
        assertTrue(rules.indexOf("dport 443 accept") < rules.indexOf("boundary counter drop"));
        assertThrows(IllegalArgumentException.class, () -> NetworkIsolationRules.compile("bad; accept", List.of("br-test")));
        assertThrows(IllegalArgumentException.class, () -> NetworkIsolationRules.compile("floci_iso_test", List.of("br-test;accept")));
    }
}
