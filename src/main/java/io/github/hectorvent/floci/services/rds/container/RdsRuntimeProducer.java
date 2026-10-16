package io.github.hectorvent.floci.services.rds.container;

import io.github.hectorvent.floci.config.EmulatorConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;

@ApplicationScoped
public class RdsRuntimeProducer {
    @Produces
    @Singleton
    RdsBackendRuntime runtime(EmulatorConfig config, RdsContainerManager docker,
                              Instance<MicrosandboxRdsRuntime> microsandbox) {
        return switch (config.services().rds().runtime()) {
            case "docker" -> docker;
            case "microsandbox" -> microsandbox.get();
            default -> throw new IllegalArgumentException("floci.services.rds.runtime must be docker or microsandbox");
        };
    }
}
