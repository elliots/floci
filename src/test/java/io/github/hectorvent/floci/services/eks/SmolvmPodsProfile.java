package io.github.hectorvent.floci.services.eks;

import io.github.hectorvent.floci.runtime.flocinode.FakeNodeProfile;

import java.util.Map;

/**
 * Runs EKS with the {@code smolvm-pods} provider and no VM, on the fakes of
 * {@link FakeNodeProfile}: {@code node-vm.sh} creates a node that answers with a fake
 * Kubernetes API and a fake node service.
 */
public class SmolvmPodsProfile extends FakeNodeProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
        return overrides();
    }

    /** The settings of the smolvm-pods provider on the fakes, for this profile and the ones that extend it. */
    public static Map<String, String> overrides() {
        Map<String, String> overrides = FakeNodeProfile.overrides();
        overrides.put("floci.services.eks.mock", "false");
        overrides.put("floci.services.eks.provider", EksSmolvmSettings.PROVIDER_SMOLVM_PODS);
        overrides.put("floci.services.eks.data-path", FakeNodeProfile.overrides().get("floci.runtime.node.data-path")
                + "-eks");
        return overrides;
    }
}
