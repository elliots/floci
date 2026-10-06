package io.github.hectorvent.floci.runtime.ondemand;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.services.sqs.SqsService;
import io.vertx.core.Vertx;
import jakarta.enterprise.inject.Instance;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class OnDemandConfigurationServiceTest {
    @Test
    @SuppressWarnings("unchecked")
    void requiresItsOwnFileWithoutConsultingNetworkConfiguration() {
        EmulatorConfig config = mock(EmulatorConfig.class, RETURNS_DEEP_STUBS);
        when(config.onDemand().enabled()).thenReturn(true);
        when(config.onDemand().configFile()).thenReturn(Optional.empty());
        when(config.onDemand().gatewayHost()).thenReturn("127.0.0.1");
        when(config.onDemand().gatewayPort()).thenReturn(8080);
        when(config.onDemand().reconcileIntervalMillis()).thenReturn(1000L);
        when(config.onDemand().maxConcurrentStarts()).thenReturn(1);
        when(config.onDemand().maxPendingRequests()).thenReturn(1);
        when(config.onDemand().maxPendingRequestsPerWorkload()).thenReturn(1);
        Instance<SqsService> sqs = mock(Instance.class);
        OnDemandService service = new OnDemandService(config, sqs, mock(WorkloadRuntimeFactory.class), mock(Vertx.class));
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> service.start(null));
        assertEquals("floci.on-demand.config-file is required", error.getMessage());
        verify(config, never()).network();
    }
}
