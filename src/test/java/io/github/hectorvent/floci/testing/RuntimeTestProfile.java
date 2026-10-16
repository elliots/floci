package io.github.hectorvent.floci.testing;

import java.util.Map;

/** Selects a real execution backend for the shared integration contracts. */
public final class RuntimeTestProfile {
    private RuntimeTestProfile() {
    }

    public static String backend() {
        String backend = System.getenv().getOrDefault("FLOCI_TEST_RUNTIME", "docker");
        if (!backend.equals("docker") && !backend.equals("microsandbox")) {
            throw new IllegalArgumentException("FLOCI_TEST_RUNTIME must be docker or microsandbox");
        }
        return backend;
    }

    public static void configureRds(Map<String, String> overrides) {
        String prefix = "floci.services.rds.";
        overrides.put(prefix + "runtime", backend());
        overrides.put(prefix + "mock", "false");
        overrides.put("floci.docker.resource-namespace", "runtime-contract-" + backend()
                + "-" + ProcessHandle.current().pid());
        if (backend().equals("microsandbox")) {
            overrides.put(prefix + "microsandbox-url", requiredEnvironment("FLOCI_TEST_MICROSANDBOX_URL"));
            overrides.put(prefix + "microsandbox-token", requiredEnvironment("FLOCI_TEST_MICROSANDBOX_TOKEN"));
        }
    }

    private static String requiredEnvironment(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required for the microsandbox test backend");
        }
        return value;
    }
}
