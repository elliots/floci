package io.github.hectorvent.floci.runtime.ondemand;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.Set;

public class OnDemandQueueTestProfile implements QuarkusTestProfile {
    @Override
    public Set<Class<?>> getEnabledAlternatives() {
        return Set.of(QueueRuntimeTestFactory.class);
    }
}
