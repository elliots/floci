package io.github.hectorvent.floci.services.eks;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.config.EmulatorConfig.EksServiceConfig;
import io.github.hectorvent.floci.config.EmulatorConfig.EksServiceConfig.SmolvmConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class EksSmolvmSettingsServiceTest {

    private EksServiceConfig eks;
    private SmolvmConfig smolvm;
    private EksSmolvmSettings settings;

    @BeforeEach
    void setUp() {
        EmulatorConfig config = mock(EmulatorConfig.class);
        EmulatorConfig.ServicesConfig services = mock(EmulatorConfig.ServicesConfig.class);
        eks = mock(EksServiceConfig.class);
        smolvm = mock(SmolvmConfig.class);
        when(config.services()).thenReturn(services);
        when(services.eks()).thenReturn(eks);
        when(eks.smolvm()).thenReturn(smolvm);
        when(eks.enabled()).thenReturn(true);
        when(eks.provider()).thenReturn("smolvm-pods");
        when(smolvm.kubernetesVersion()).thenReturn("1.35");
        settings = new EksSmolvmSettings(config);
    }

    @Test
    void bothProvidersPass() {
        assertDoesNotThrow(settings::requireValid);
        when(eks.provider()).thenReturn("k3s");
        assertDoesNotThrow(settings::requireValid);
    }

    @Test
    void anUnknownProviderIsRefused() {
        when(eks.provider()).thenReturn("k3d");

        IllegalStateException refused = assertThrows(IllegalStateException.class, settings::requireValid);

        assertEquals("floci.services.eks.provider (FLOCI_SERVICES_EKS_PROVIDER) must be k3s or smolvm-pods, "
                + "not \"k3d\"", refused.getMessage());
    }

    @Test
    void theReportedVersionIsAKubernetesMinor() {
        when(smolvm.kubernetesVersion()).thenReturn("v1.35");

        IllegalStateException refused = assertThrows(IllegalStateException.class, settings::requireValid);

        assertEquals("floci.services.eks.smolvm.kubernetes-version (FLOCI_SERVICES_EKS_SMOLVM_KUBERNETES_VERSION) "
                + "must be a version such as 1.35, not \"v1.35\"", refused.getMessage());
    }

    @Test
    void disabledEksChecksNothing() {
        when(eks.enabled()).thenReturn(false);
        when(eks.provider()).thenReturn("k3d");

        assertDoesNotThrow(settings::requireValid);
    }
}
