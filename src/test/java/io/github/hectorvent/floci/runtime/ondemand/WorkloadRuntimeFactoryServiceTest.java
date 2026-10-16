package io.github.hectorvent.floci.runtime.ondemand;

import io.github.hectorvent.floci.runtime.ondemand.WorkloadDefinition.RuntimeType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class WorkloadRuntimeFactoryServiceTest {
    @Test
    void kubernetesTargetRequiresExplicitPodControllerBeforeCreatingAGuest() {
        MicrosandboxWorkloadProvider provider = mock(MicrosandboxWorkloadProvider.class);
        WorkloadRuntimeFactory factory = new WorkloadRuntimeFactory(provider);
        WorkloadDefinition definition = OnDemandTestSupport.definition("worker", null, List.of("events"));
        assertEquals(RuntimeType.KUBERNETES, definition.type());
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, () -> factory.create(definition));
        assertTrue(failure.getMessage().contains("native pod controller"));
        verifyNoInteractions(provider);
    }
}
