package io.github.hectorvent.floci.services.rds.container;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

@QuarkusTest
class RdsBackendRuntimeIntegrationTest {

    @Inject
    RdsBackendRuntime runtime;

    @Inject
    RdsBackendGate gate;

    @Test
    void lifecycleAndConnectionGateUseTheSameDefaultProvider() {
        assertInstanceOf(RdsContainerManager.class, runtime);
        assertSame(runtime, gate);
    }
}
