package io.github.hectorvent.floci.runtime.ondemand;

import io.github.hectorvent.floci.runtime.ondemand.OnDemandTestSupport.FakeRuntime;
import io.github.hectorvent.floci.services.ec2.Ec2Service;
import io.github.hectorvent.floci.services.lambda.launcher.kubernetes.KubernetesApiClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Alternative
@ApplicationScoped
public class QueueRuntimeTestFactory extends WorkloadRuntimeFactory {
    private final Map<String, FakeRuntime> runtimes = new ConcurrentHashMap<>();

    public QueueRuntimeTestFactory() {
        super(null, null);
    }

    @Inject
    public QueueRuntimeTestFactory(Ec2Service ec2, KubernetesApiClient kubernetes) {
        super(ec2, kubernetes);
    }

    @Override
    public WorkloadRuntime create(WorkloadDefinition definition) {
        FakeRuntime runtime = new FakeRuntime();
        runtime.backend = definition.backendUrl();
        runtimes.put(definition.id(), runtime);
        return runtime;
    }

    public FakeRuntime runtime(String id) {
        return runtimes.get(id);
    }
}
