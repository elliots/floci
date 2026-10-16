package io.github.hectorvent.floci.runtime.ondemand;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.microsandbox.PodControllerClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class WorkloadRuntimeFactory {
    private final MicrosandboxWorkloadProvider microsandbox;
    private final EmulatorConfig config;
    private final ObjectMapper mapper;
    private PodControllerClient podController;

    public WorkloadRuntimeFactory(MicrosandboxWorkloadProvider microsandbox) {
        this(microsandbox, null, new ObjectMapper());
    }

    @Inject
    public WorkloadRuntimeFactory(MicrosandboxWorkloadProvider microsandbox, EmulatorConfig config, ObjectMapper mapper) {
        this.microsandbox = microsandbox;
        this.config = config;
        this.mapper = mapper;
    }

    private synchronized PodControllerClient podController() {
        if (podController == null) {
            if (config == null) {
                throw new IllegalArgumentException("Kubernetes workload requires a native pod controller");
            }
            String url = config.onDemand().podControllerUrl().orElseThrow(() ->
                    new IllegalArgumentException("Set FLOCI_ON_DEMAND_POD_CONTROLLER_URL"));
            String token = config.onDemand().podControllerToken().orElseThrow(() ->
                    new IllegalArgumentException("Set FLOCI_ON_DEMAND_POD_CONTROLLER_TOKEN"));
            podController = new PodControllerClient(url, token, mapper);
        }
        return podController;
    }

    public WorkloadRuntime create(WorkloadDefinition definition) {
        return switch (definition.type()) {
            case MICROSANDBOX -> microsandbox.create(definition);
            case KUBERNETES -> new KubernetesWorkloadRuntime(definition, podController());
            default -> throw new IllegalArgumentException("Workload runtime is not implemented: " + definition.type());
        };
    }

    public void clear() {
        microsandbox.clear();
    }
}
