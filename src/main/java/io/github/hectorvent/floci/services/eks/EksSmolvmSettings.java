package io.github.hectorvent.floci.services.eks;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.config.EmulatorConfig.EksServiceConfig;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

/**
 * The EKS provider, {@code floci.services.eks.provider}, and the settings of a cluster on the
 * smolvm-pods node, {@code floci.services.eks.smolvm.*}. The node's own settings are
 * {@code floci.runtime.node.*}, which {@code NodeSettings} checks.
 */
@ApplicationScoped
public class EksSmolvmSettings {

    static final String PROVIDER_K3S = "k3s";
    static final String PROVIDER_SMOLVM_PODS = "smolvm-pods";

    private final EmulatorConfig config;

    @Inject
    public EksSmolvmSettings(EmulatorConfig config) {
        this.config = config;
    }

    void onStart(@Observes StartupEvent ignored) {
        requireValid();
    }

    /**
     * Refuses a provider other than {@code k3s} and {@code smolvm-pods}, and a
     * {@code kubernetes-version} that is not a version.
     *
     * @throws IllegalStateException naming the problem
     */
    void requireValid() {
        EksServiceConfig eks = config.services().eks();
        if (!eks.enabled()) {
            return;
        }
        String provider = eks.provider();
        if (!PROVIDER_K3S.equals(provider) && !PROVIDER_SMOLVM_PODS.equals(provider)) {
            throw new IllegalStateException("floci.services.eks.provider (FLOCI_SERVICES_EKS_PROVIDER) must be "
                    + PROVIDER_K3S + " or " + PROVIDER_SMOLVM_PODS + ", not \"" + provider + "\"");
        }
        String version = eks.smolvm().kubernetesVersion();
        if (smolvmProvider(config) && !EksService.K8S_VERSION_PATTERN.matcher(version).matches()) {
            throw new IllegalStateException("floci.services.eks.smolvm.kubernetes-version "
                    + "(FLOCI_SERVICES_EKS_SMOLVM_KUBERNETES_VERSION) must be a version such as 1.35, not \""
                    + version + "\"");
        }
    }

    /** Whether new clusters run on the smolvm-pods node: the provider is {@code smolvm-pods}. */
    static boolean smolvmProvider(EmulatorConfig config) {
        return config != null && config.services() != null && config.services().eks() != null
                && PROVIDER_SMOLVM_PODS.equals(config.services().eks().provider());
    }
}
