package io.github.hectorvent.floci.runtime.network;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.hectorvent.floci.config.FlociCertificateAuthority.RouteIssuer;
import io.github.hectorvent.floci.core.common.Digests;
import io.github.hectorvent.floci.runtime.network.NetworkDefinitions.Allow;
import io.github.hectorvent.floci.runtime.network.NetworkDefinitions.Origin;
import io.github.hectorvent.floci.runtime.network.NetworkDefinitions.Route;
import io.github.hectorvent.floci.runtime.node.SmolvmNode;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.List;
import java.util.TreeSet;

/**
 * The network profile of one node, as {@code PUT /v1/network} takes it
 * ({@code tools/smolvm-pods/tests/fixtures/network-profile.json} is the contract).
 */
public final class NodeNetworkProfile {

    private NodeNetworkProfile() {
    }

    /** The names that a route intermediate must permit: each exact host and each wildcard's domain. */
    public static List<String> permittedNames(NetworkDefinitions network) {
        TreeSet<String> names = new TreeSet<>();
        for (Origin origin : network.origins()) {
            names.add(origin.host());
        }
        return List.copyOf(names);
    }

    /**
     * The profile for {@code node}.
     *
     * @param issuer the route intermediate, required when {@code network} has routes
     */
    public static ObjectNode build(ObjectMapper mapper, SmolvmNode node, NetworkDefinitions network,
                                   List<FlociPortInventory.PortRange> flociPorts, int dnsPort,
                                   RouteIssuer issuer, NetworkTrustMaterial.Material trust) {
        ObjectNode profile = mapper.createObjectNode();
        profile.put("isolation", network.isolation());
        ObjectNode floci = profile.putObject("floci");
        floci.put("dns_port", dnsPort);
        ArrayNode ports = floci.putArray("ports");
        for (FlociPortInventory.PortRange range : flociPorts) {
            ports.add(range.wire());
        }
        ArrayNode endpoints = profile.putArray("endpoints");
        for (SmolvmNode.Endpoint endpoint : node.endpoints()) {
            endpoints.addObject().put("address", endpoint.address()).put("port", endpoint.port())
                    .put("target_port", endpoint.targetPort());
        }
        ArrayNode routes = profile.putArray("routes");
        for (Route route : network.routes()) {
            ObjectNode r = routes.addObject();
            r.put("name", route.name());
            ArrayNode origins = r.putArray("origins");
            route.origins().forEach(origin -> origins.add(origin.wire()));
            r.put("backend", route.backendHost() + ":" + route.backendPort());
            r.put("protocol", route.protocol().wire());
        }
        ArrayNode allow = profile.putArray("allow");
        for (Allow entry : network.allow()) {
            ObjectNode a = allow.addObject();
            a.put("cidr", entry.cidr());
            a.put("protocol", entry.protocol().wire());
            ArrayNode entryPorts = a.putArray("ports");
            entry.ports().forEach(entryPorts::add);
        }
        if (issuer != null && !network.routes().isEmpty()) {
            profile.putObject("issuer").put("certificate", issuer.certificatePem()).put("key", issuer.keyPem())
                    .put("subject", issuer.subject()).put("key_id", issuer.keyId())
                    .put("not_after", issuer.notAfter().toString());
        }
        if (trust != null) {
            profile.putObject("trust").put("mode", network.trustMode().wire()).put("ca", trust.ca())
                    .put("ca_sha256", trust.caSha256()).put("bundle", trust.bundle())
                    .put("pkcs12", trust.pkcs12()).put("jks", trust.jks());
        }
        String digest = HexFormat.of().formatHex(Digests.sha256(profile.toString().getBytes(StandardCharsets.UTF_8)));
        ObjectNode withGeneration = mapper.createObjectNode();
        withGeneration.put("generation", digest);
        withGeneration.setAll(profile);
        return withGeneration;
    }
}
