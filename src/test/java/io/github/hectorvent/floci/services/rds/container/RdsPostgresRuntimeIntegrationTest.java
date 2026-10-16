package io.github.hectorvent.floci.services.rds.container;

import io.github.hectorvent.floci.testing.RdsRuntimeTestProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.RestAssured;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DBInstance;

import java.net.URI;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@TestProfile(RdsRuntimeTestProfile.class)
@Tag("runtime")
class RdsPostgresRuntimeIntegrationTest extends RdsRuntimeTestSupport {
    @BeforeAll
    static void loadJdbcDriverForThisProfile() throws ClassNotFoundException {
        Class.forName("org.postgresql.Driver");
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void sdkAuroraClusterPausesTwiceAndKeepsProcessMemoryAndNewWrites() throws Exception {
        String id = "runtime-" + UUID.randomUUID().toString().substring(0, 8);
        try (RdsClient rds = client()) {
            DBCluster cluster = rds.createDBCluster(request -> request.dbClusterIdentifier(id)
                    .engine("aurora-postgresql").masterUsername("master").masterUserPassword("Master123!")
                    .databaseName("appdb").serverlessV2ScalingConfiguration(
                            scaling -> scaling.minCapacity(0.0).maxCapacity(2.0).secondsUntilAutoPause(300))).dbCluster();
            rds.createDBInstance(request -> request.dbInstanceIdentifier(id + "-member")
                    .dbClusterIdentifier(id).engine("aurora-postgresql").dbInstanceClass("db.serverless"));
            int port = cluster.port();
            String started;
            try (Connection connection = connect(port)) {
                execute(connection, "CREATE TABLE probe(id integer PRIMARY KEY)");
                execute(connection, "INSERT INTO probe VALUES (1)");
                execute(connection, "COPY (SELECT 'memory-survived') TO '/dev/shm/floci-probe'");
                started = scalar(connection, "SELECT pg_postmaster_start_time()::text");
                assertFalse(pauseIfIdle(cluster.dbClusterArn()), "An active SQL connection prevents pause");
            }
            // The proxy releases its lease asynchronously when it observes the client EOF.
            for (int cycle = 1; cycle <= 2; cycle++) {
                awaitIdlePause(cluster.dbClusterArn());
                assertBackendPaused(cluster.dbClusterArn());
                assertEquals("available", rds.describeDBClusters(request -> request.dbClusterIdentifier(id))
                        .dbClusters().getFirst().status());
                try (Connection connection = connect(port)) {
                    assertBackendRunning(cluster.dbClusterArn());
                    assertEquals(started, scalar(connection, "SELECT pg_postmaster_start_time()::text"));
                    assertEquals("memory-survived\n", scalar(connection, "SELECT pg_read_file('/dev/shm/floci-probe')"));
                    assertEquals(Integer.toString(cycle), scalar(connection, "SELECT count(*) FROM probe"));
                    execute(connection, "INSERT INTO probe VALUES (" + (cycle + 1) + ")");
                }
            }
            assertEquals(3, Integer.parseInt(query(port, "SELECT count(*) FROM probe")));
            rds.stopDBCluster(request -> request.dbClusterIdentifier(id));
            rds.startDBCluster(request -> request.dbClusterIdentifier(id));
            String restarted;
            try (Connection connection = connect(port)) {
                restarted = scalar(connection, "SELECT pg_postmaster_start_time()::text");
                assertNotEquals(started, restarted);
                assertEquals("3", scalar(connection, "SELECT count(*) FROM probe"));
                execute(connection, "COPY (SELECT 'fresh-memory-survived') TO '/dev/shm/floci-probe'");
            }
            awaitIdlePause(cluster.dbClusterArn());
            assertBackendPaused(cluster.dbClusterArn());
            try (Connection connection = connect(port)) {
                assertBackendRunning(cluster.dbClusterArn());
                assertEquals(restarted, scalar(connection, "SELECT pg_postmaster_start_time()::text"));
                assertEquals("fresh-memory-survived\n", scalar(connection, "SELECT pg_read_file('/dev/shm/floci-probe')"));
                assertEquals("3", scalar(connection, "SELECT count(*) FROM probe"));
            }
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void sdkStopStartBootsFreshProcessesAndRetainsData() throws Exception {
        String id = "runtime-stop-" + UUID.randomUUID().toString().substring(0, 8);
        try (RdsClient rds = client()) {
            DBInstance instance = rds.createDBInstance(request -> request.dbInstanceIdentifier(id)
                    .engine("postgres").dbInstanceClass("db.t3.micro").allocatedStorage(5)
                    .masterUsername("master").masterUserPassword("Master123!").dbName("appdb")).dbInstance();
            int port = instance.endpoint().port();
            String started;
            try (Connection connection = connect(port)) {
                execute(connection, "CREATE TABLE probe(id integer)");
                execute(connection, "INSERT INTO probe VALUES (42)");
                started = scalar(connection, "SELECT pg_postmaster_start_time()::text");
            }
            rds.stopDBInstance(request -> request.dbInstanceIdentifier(id));
            rds.startDBInstance(request -> request.dbInstanceIdentifier(id));
            assertNotEquals(started, query(port, "SELECT pg_postmaster_start_time()::text"));
            assertEquals("42", query(port, "SELECT id FROM probe"));
            rds.modifyDBInstance(request -> request.dbInstanceIdentifier(id)
                    .masterUserPassword("Changed123!").applyImmediately(true));
            rds.createDBSnapshot(request -> request.dbInstanceIdentifier(id).dbSnapshotIdentifier(id + "-snap"));
            try (AutoCloseable snapshotCleanup = () -> rds.deleteDBSnapshot(
                    request -> request.dbSnapshotIdentifier(id + "-snap"))) {
                DBInstance restored = rds.restoreDBInstanceFromDBSnapshot(request -> request
                        .dbInstanceIdentifier(id + "-copy").dbSnapshotIdentifier(id + "-snap")
                        .dbInstanceClass("db.t3.micro")).dbInstance();
                try (Connection connection = postgres(restored.endpoint().port(), "master", "Changed123!")) {
                    assertEquals("42", scalar(connection, "SELECT id FROM probe"));
                }
            }
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void sdkSnapshotRestoresTheDefaultDatabaseWhenDbNameIsOmitted() throws Exception {
        String id = "runtime-default-" + UUID.randomUUID().toString().substring(0, 8);
        try (RdsClient rds = client()) {
            DBInstance source = rds.createDBInstance(request -> request.dbInstanceIdentifier(id)
                    .engine("postgres").dbInstanceClass("db.t3.micro").allocatedStorage(5)
                    .masterUsername("postgres").masterUserPassword("Master123!")).dbInstance();
            try (Connection connection = postgres(source.endpoint().port(), "postgres", "postgres", "Master123!")) {
                execute(connection, "CREATE TABLE marker(value integer)");
                execute(connection, "INSERT INTO marker VALUES (43)");
            }
            rds.createDBSnapshot(request -> request.dbInstanceIdentifier(id).dbSnapshotIdentifier(id + "-snap"));
            try (AutoCloseable snapshotCleanup = () -> rds.deleteDBSnapshot(
                    request -> request.dbSnapshotIdentifier(id + "-snap"))) {
                DBInstance restored = rds.restoreDBInstanceFromDBSnapshot(request -> request
                        .dbInstanceIdentifier(id + "-copy").dbSnapshotIdentifier(id + "-snap")).dbInstance();
                assertEquals(id + "-copy", rds.describeDBInstances(request -> request
                        .dbInstanceIdentifier(id + "-copy")).dbInstances().getFirst().dbInstanceIdentifier());
                try (Connection connection = postgres(restored.endpoint().port(), "postgres", "postgres", "Master123!")) {
                    assertEquals("43", scalar(connection, "SELECT value FROM marker"));
                }
            }
        }
    }

    private void awaitIdlePause(String arn) throws InterruptedException {
        for (int attempt = 0; attempt < 40; attempt++) {
            if (pauseIfIdle(arn)) {
                return;
            }
            Thread.sleep(50);
        }
        assertTrue(pauseIfIdle(arn), "Idle cluster should pause");
    }

    private static RdsClient client() {
        return RdsClient.builder().endpointOverride(URI.create("http://localhost:" + RestAssured.port))
                .httpClientBuilder(UrlConnectionHttpClient.builder().socketTimeout(Duration.ofMinutes(4)))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("000000000000", "test"))).build();
    }

    private static Connection connect(int port) throws Exception {
        return postgres(port, "master", "Master123!");
    }

    private static String query(int port, String sql) throws Exception {
        try (Connection connection = connect(port)) {
            return scalar(connection, sql);
        }
    }

    private static void execute(Connection connection, String sql) throws Exception {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private static String scalar(Connection connection, String sql) throws Exception {
        try (Statement statement = connection.createStatement(); ResultSet result = statement.executeQuery(sql)) {
            assertTrue(result.next());
            return result.getString(1);
        }
    }
}
