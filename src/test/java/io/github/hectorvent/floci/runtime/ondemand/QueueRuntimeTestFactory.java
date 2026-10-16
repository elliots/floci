package io.github.hectorvent.floci.runtime.ondemand;

import io.github.hectorvent.floci.runtime.ondemand.OnDemandTestSupport.FakeRuntime;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Alternative
@ApplicationScoped
public class QueueRuntimeTestFactory extends WorkloadRuntimeFactory {
    private final Map<String, FakeRuntime> runtimes = new ConcurrentHashMap<>();

    public QueueRuntimeTestFactory() {
        super(null);
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

    @Override
    public void clear() {
        runtimes.clear();
    }
}
