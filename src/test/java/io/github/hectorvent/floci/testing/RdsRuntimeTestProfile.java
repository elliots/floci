package io.github.hectorvent.floci.testing;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.HashMap;
import java.util.Map;

public class RdsRuntimeTestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
        Map<String, String> overrides = new HashMap<>();
        RuntimeTestProfile.configureRds(overrides);
        overrides.put("floci.services.rds.default-postgres-image",
                System.getenv().getOrDefault("FLOCI_TEST_POSTGRES_IMAGE", "postgres:17.11-alpine"));
        overrides.put("floci.services.rds.default-mysql-image",
                System.getenv().getOrDefault("FLOCI_TEST_MYSQL_IMAGE", "mysql:8.0.45"));
        overrides.put("floci.services.rds.aurora-resume-delay-millis", "0");
        return overrides;
    }
}
