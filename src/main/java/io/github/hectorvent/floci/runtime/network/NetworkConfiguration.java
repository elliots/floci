package io.github.hectorvent.floci.runtime.network;

import io.github.hectorvent.floci.config.EmulatorConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.nio.file.Path;
import java.util.Optional;

/** Loads immutable network configuration at startup. */
@ApplicationScoped
public class NetworkConfiguration {
    private final NetworkDefinition network;
    private final boolean configured;
    private volatile String gatewayAddress;

    @Inject
    public NetworkConfiguration(EmulatorConfig config) {
        Optional<String> path = config.network().configFile();
        configured = path.isPresent();
        network = path.map(value -> NetworkConfigLoader.load(Path.of(value))).orElse(NetworkDefinition.DISABLED);
        if (network.isolated() && "kubernetes".equals(config.services().lambda().executor())) {
            throw new IllegalArgumentException("Workload isolation does not support the external Kubernetes Lambda executor; "
                    + "use the Docker executor");
        }
    }

    public boolean configured() {
        return configured;
    }

    public NetworkDefinition network() {
        return network;
    }

    public Optional<String> gatewayAddress() {
        return Optional.ofNullable(gatewayAddress);
    }

    void gatewayAddress(String address) {
        gatewayAddress = address;
    }
}
