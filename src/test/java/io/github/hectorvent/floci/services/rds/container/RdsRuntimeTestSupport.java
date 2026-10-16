package io.github.hectorvent.floci.services.rds.container;

import com.fasterxml.jackson.core.type.TypeReference;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.exception.NotFoundException;
import io.github.hectorvent.floci.core.storage.AccountAwareStorageBackend;
import io.github.hectorvent.floci.core.storage.StorageFactory;
import io.github.hectorvent.floci.testing.RuntimeTestProfile;
import io.restassured.RestAssured;
import io.restassured.config.RestAssuredConfig;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.postgresql.Driver;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Backend mechanics are confined here; the contracts use AWS APIs and database endpoints. */
@Timeout(value = 10, unit = TimeUnit.MINUTES)
@ResourceLock("rds-runtime-contract")
public abstract class RdsRuntimeTestSupport {
    @Inject
    RdsBackendRuntime runtime;

    @Inject
    DockerClient docker;

    @Inject
    MicrosandboxSidecarClient nativeClient;

    @Inject
    StorageFactory storageFactory;

    private RestAssuredConfig previousConfig;
    private RdsRuntimeCleanup resourceCleanup;
    private Driver postgresDriver;

    @RegisterExtension
    final AfterTestExecutionCallback cleanupCreatedResources = context -> {
        if (resourceCleanup != null) {
            resourceCleanup.cleanup(context.getExecutionException().orElse(null));
        }
    };

    @BeforeEach
    void requireSelectedBackend() throws SQLException {
        boolean reachable = runtime.isDockerReachable();
        if (System.getenv("FLOCI_TEST_RUNTIME") == null) {
            assumeTrue(reachable, "Docker is not available");
        } else {
            assertTrue(reachable, "The explicitly selected " + RuntimeTestProfile.backend() + " backend must be available");
        }
        previousConfig = RestAssured.config;
        RestAssured.config = previousConfig.httpClient(previousConfig.getHttpClientConfig()
                .setParam("http.socket.timeout", 240_000));
        resourceCleanup = new RdsRuntimeCleanup(storageFactory, URI.create("http://localhost:" + RestAssured.port));
        // DriverManager otherwise retains a driver from an earlier Quarkus profile's classloader.
        postgresDriver = new Driver();
        DriverManager.registerDriver(postgresDriver);
    }

    @AfterEach
    void restoreClientTimeout() throws SQLException {
        try {
            if (postgresDriver != null) {
                DriverManager.deregisterDriver(postgresDriver);
                postgresDriver = null;
            }
        } finally {
            if (previousConfig != null) {
                RestAssured.config = previousConfig;
            }
        }
    }

    protected boolean pauseIfIdle(String runtimeId) {
        return switch (runtime) {
            case RdsContainerManager container -> container.pauseIfIdle(runtimeId);
            case MicrosandboxRdsRuntime microsandbox -> microsandbox.pauseIfIdle(runtimeId);
            default -> throw new IllegalStateException("No idle-clock fixture for " + runtime.getClass());
        };
    }

    protected void assertBackendPaused(String runtimeId) {
        if (runtime instanceof RdsContainerManager) {
            InspectContainerResponse.ContainerState state = dockerState(runtimeId);
            assertEquals(Boolean.TRUE, state.getRunning());
            assertEquals(Boolean.TRUE, state.getPaused(), "The database container must actually be frozen");
        } else {
            MicrosandboxMachine machine = nativeMachine(runtimeId);
            assertEquals("HIBERNATED", machine.phase);
            assertNotNull(machine.snapshot, "Hibernation must retain its RAM recovery point");
            assertEquals("Stopped", nativeClient.json("inspect", "--format", "json", machine.vm)
                    .path("status").asText(), "The captured VM must release its running process");
        }
    }

    protected void assertBackendRunning(String runtimeId) {
        if (runtime instanceof RdsContainerManager) {
            InspectContainerResponse.ContainerState state = dockerState(runtimeId);
            assertEquals(Boolean.TRUE, state.getRunning());
            assertEquals(Boolean.FALSE, state.getPaused(), "The database container must actually resume");
        } else {
            MicrosandboxMachine machine = nativeMachine(runtimeId);
            assertEquals("RUNNING", machine.phase);
            assertEquals("Running", nativeClient.json("inspect", "--format", "json", machine.vm)
                    .path("status").asText(), "The restored VM must actually be running");
        }
    }

    protected void assertBackendStopped(String runtimeId, String previousBackendId) {
        if (runtime instanceof RdsContainerManager) {
            assertThrows(NotFoundException.class, () -> docker.inspectContainerCmd(previousBackendId).exec(),
                    "Stopping a Docker database must remove the old process container");
        } else {
            MicrosandboxMachine machine = nativeMachine(runtimeId);
            assertEquals("STOPPED", machine.phase);
            assertEquals("Stopped", nativeClient.json("inspect", "--format", "json", machine.vm)
                    .path("status").asText(), "Stopping a native database must stop its VM process");
        }
    }

    private InspectContainerResponse.ContainerState dockerState(String runtimeId) {
        RdsContainerHandle handle = runtime.getActiveHandle(runtimeId);
        assertNotNull(handle, "The runtime must retain its active database identity");
        return docker.inspectContainerCmd(handle.getContainerId()).exec().getState();
    }

    private MicrosandboxMachine nativeMachine(String runtimeId) {
        AccountAwareStorageBackend<MicrosandboxMachine> journal = storageFactory.create("rds",
                "rds-microsandbox.json", new TypeReference<Map<String, MicrosandboxMachine>>() {});
        return journal.scanAllAccounts().stream().filter(machine -> runtimeId.equals(machine.runtimeId))
                .findFirst().orElseThrow(() -> new AssertionError("Missing native database ownership journal"));
    }

    protected static Connection postgres(int port, String username, String password) throws SQLException {
        return postgres(port, "appdb", username, password);
    }

    protected static Connection postgres(int port, String database, String username, String password) throws SQLException {
        Properties credentials = new Properties();
        credentials.setProperty("user", username);
        credentials.setProperty("password", password);
        credentials.setProperty("connectTimeout", "60");
        // A directly constructed driver survives Quarkus test-profile classloader changes.
        return new Driver().connect("jdbc:postgresql://127.0.0.1:" + port + "/" + database, credentials);
    }

    protected static String postgresSql(int port, String sql) throws SQLException {
        return postgresSql(port, "appdb", sql);
    }

    protected static String postgresSql(int port, String database, String sql) throws SQLException {
        try (Connection connection = postgres(port, database, "admin", "password123");
             Statement statement = connection.createStatement()) {
            boolean rows = statement.execute(sql);
            StringBuilder output = new StringBuilder();
            while (true) {
                if (rows) {
                    try (ResultSet result = statement.getResultSet()) {
                        while (result.next()) {
                            output.append(result.getString(1)).append('\n');
                        }
                    }
                } else if (statement.getUpdateCount() == -1) {
                    return output.toString();
                }
                rows = statement.getMoreResults();
            }
        }
    }
}
