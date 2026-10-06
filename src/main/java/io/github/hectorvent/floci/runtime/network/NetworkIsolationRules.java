package io.github.hectorvent.floci.runtime.network;

import java.util.Collection;
import java.util.stream.Collectors;

/** An outer boundary on Docker bridges, independent of rules inside EC2 or Kubernetes nodes. */
final class NetworkIsolationRules {
    private NetworkIsolationRules() {}

    static String compile(String table, Collection<String> bridges) {
        if (!table.matches("floci_iso_[a-z0-9]+") || bridges.isEmpty()
                || bridges.stream().anyMatch(bridge -> !bridge.matches("[a-zA-Z0-9_-]{1,15}"))) {
            throw new IllegalArgumentException("Invalid isolation table or bridge name");
        }
        String interfaces = bridges.stream().map(bridge -> "\"" + bridge + "\"")
                .collect(Collectors.joining(", "));
        String prefix = "add rule inet " + table + " ";
        return "add table inet " + table + "\n"
                + "flush table inet " + table + "\n"
                + "add chain inet " + table + " forward { type filter hook forward priority -200; policy accept; }\n"
                + "add chain inet " + table + " input { type filter hook input priority -200; policy accept; }\n"
                + "add chain inet " + table + " boundary\n"
                + prefix + "forward iifname { " + interfaces + " } jump boundary\n"
                + prefix + "input iifname { " + interfaces + " } jump boundary\n"
                + prefix + "boundary ct state new meta nftrace set 1\n"
                + prefix + "boundary ct direction reply ct state established,related accept\n"
                + prefix + "boundary oifname { " + interfaces + " } accept\n"
                + prefix + "boundary counter drop\n";
    }
}
