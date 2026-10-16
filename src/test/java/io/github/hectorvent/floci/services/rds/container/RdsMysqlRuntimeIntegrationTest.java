package io.github.hectorvent.floci.services.rds.container;

import com.mysql.cj.jdbc.Driver;
import io.github.hectorvent.floci.testing.RdsRuntimeTestProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.RestAssured;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@TestProfile(RdsRuntimeTestProfile.class)
@Tag("runtime")
class RdsMysqlRuntimeIntegrationTest extends RdsRuntimeTestSupport {
    private static final String INITIAL_PASSWORD = "Master123!";
    private static final String CHANGED_PASSWORD = "Changed123!";
    private static final String ROTATED_PASSWORD = "Rotated123!";

    @BeforeAll
    static void loadJdbcDriverForThisProfile() throws ClassNotFoundException {
        Class.forName("com.mysql.cj.jdbc.Driver");
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    void sdkAuroraMysqlPausesTwiceAndKeepsMemoryTablesAndNewWrites() throws Exception {
        String id = "runtime-mysql-" + UUID.randomUUID().toString().substring(0, 8);
        try (RdsClient rds = client()) {
            DBCluster cluster = rds.createDBCluster(request -> request.dbClusterIdentifier(id)
                    .engine("aurora-mysql").engineVersion("8.0.mysql_aurora.3.08.0")
                    .masterUsername("master").masterUserPassword(INITIAL_PASSWORD).databaseName("appdb")
                    .serverlessV2ScalingConfiguration(scaling -> scaling
                            .minCapacity(0.0).maxCapacity(2.0).secondsUntilAutoPause(300))).dbCluster();
            rds.createDBInstance(request -> request.dbInstanceIdentifier(id + "-member")
                    .dbClusterIdentifier(id).engine("aurora-mysql").dbInstanceClass("db.serverless"));
            int port = cluster.port();
            awaitSql(port, "master", INITIAL_PASSWORD);
            try (Connection connection = connect(port, "master", INITIAL_PASSWORD)) {
                createProbes(connection);
                assertFalse(pauseIfIdle(cluster.dbClusterArn()),
                        "An active SQL connection prevents pause");
            }
            for (int cycle = 1; cycle <= 2; cycle++) {
                awaitIdlePause(cluster.dbClusterArn());
                assertBackendPaused(cluster.dbClusterArn());
                assertEquals("available", rds.describeDBClusters(request -> request.dbClusterIdentifier(id))
                        .dbClusters().getFirst().status());
                try (Connection connection = connect(port, "master", INITIAL_PASSWORD)) {
                    assertBackendRunning(cluster.dbClusterArn());
                    assertEquals("memory-survived", scalar(connection, "SELECT marker FROM memory_probe"));
                    assertEquals(Integer.toString(cycle), scalar(connection, "SELECT count(*) FROM probe"));
                    execute(connection, "INSERT INTO probe VALUES (" + (cycle + 1) + ")");
                }
            }
            rds.modifyDBCluster(request -> request.dbClusterIdentifier(id)
                    .masterUserPassword(CHANGED_PASSWORD).applyImmediately(true));
            assertRejectedPassword(port, "master", INITIAL_PASSWORD);
            awaitIdlePause(cluster.dbClusterArn());
            assertBackendPaused(cluster.dbClusterArn());
            try (Connection connection = connect(port, "master", CHANGED_PASSWORD)) {
                assertBackendRunning(cluster.dbClusterArn());
                assertEquals("memory-survived", scalar(connection, "SELECT marker FROM memory_probe"));
                assertEquals("3", scalar(connection, "SELECT count(*) FROM probe"));
            }
            rds.stopDBCluster(request -> request.dbClusterIdentifier(id));
            rds.startDBCluster(request -> request.dbClusterIdentifier(id));
            awaitSql(port, "master", CHANGED_PASSWORD);
            try (Connection connection = connect(port, "master", CHANGED_PASSWORD)) {
                assertEquals("0", scalar(connection, "SELECT count(*) FROM memory_probe"),
                        "A cold restart starts a fresh MySQL process with empty MEMORY tables");
                assertEquals("3", scalar(connection, "SELECT count(*) FROM probe"));
                execute(connection, "INSERT INTO memory_probe VALUES ('fresh-memory-survived')");
            }
            awaitIdlePause(cluster.dbClusterArn());
            assertBackendPaused(cluster.dbClusterArn());
            try (Connection connection = connect(port, "master", CHANGED_PASSWORD)) {
                assertBackendRunning(cluster.dbClusterArn());
                assertEquals("fresh-memory-survived", scalar(connection, "SELECT marker FROM memory_probe"));
                assertEquals("3", scalar(connection, "SELECT count(*) FROM probe"));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"master", "root"})
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    void sdkMysqlColdStartsAndRotatesMasterPasswordRepeatedly(String username) throws Exception {
        String id = "runtime-mysql-stop-" + UUID.randomUUID().toString().substring(0, 8);
        try (RdsClient rds = client()) {
            DBInstance instance = rds.createDBInstance(request -> request.dbInstanceIdentifier(id)
                    .engine("mysql").engineVersion("8.0.45").dbInstanceClass("db.t3.micro").allocatedStorage(5)
                    .masterUsername(username).masterUserPassword(INITIAL_PASSWORD).dbName("appdb")).dbInstance();
            int port = instance.endpoint().port();
            awaitSql(port, username, INITIAL_PASSWORD);
            try (Connection connection = connect(port, username, INITIAL_PASSWORD)) {
                createProbes(connection);
            }
            rds.stopDBInstance(request -> request.dbInstanceIdentifier(id));
            rds.startDBInstance(request -> request.dbInstanceIdentifier(id));
            awaitSql(port, username, INITIAL_PASSWORD);
            try (Connection connection = connect(port, username, INITIAL_PASSWORD)) {
                assertEquals("0", scalar(connection, "SELECT count(*) FROM memory_probe"),
                        "Ordinary AWS stop/start starts a fresh process");
                assertEquals("1", scalar(connection, "SELECT id FROM probe"));
            }
            rds.modifyDBInstance(request -> request.dbInstanceIdentifier(id)
                    .masterUserPassword(CHANGED_PASSWORD).applyImmediately(true));
            assertRejectedPassword(port, username, INITIAL_PASSWORD);
            try (Connection connection = connect(port, username, CHANGED_PASSWORD)) {
                execute(connection, "INSERT INTO probe VALUES (2)");
            }
            rds.stopDBInstance(request -> request.dbInstanceIdentifier(id));
            rds.startDBInstance(request -> request.dbInstanceIdentifier(id));
            awaitSql(port, username, CHANGED_PASSWORD);
            try (Connection connection = connect(port, username, CHANGED_PASSWORD)) {
                assertEquals("2", scalar(connection, "SELECT count(*) FROM probe"));
            }
            rds.modifyDBInstance(request -> request.dbInstanceIdentifier(id)
                    .masterUserPassword(ROTATED_PASSWORD).applyImmediately(true));
            assertRejectedPassword(port, username, CHANGED_PASSWORD);
            try (Connection connection = connect(port, username, ROTATED_PASSWORD)) {
                assertEquals("2", scalar(connection, "SELECT count(*) FROM probe"));
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

    private static Connection connect(int port, String username, String password) throws SQLException {
        Properties credentials = new Properties();
        credentials.setProperty("user", username);
        credentials.setProperty("password", password);
        return new Driver().connect("jdbc:mysql://127.0.0.1:" + port
                + "/appdb?connectTimeout=60000&socketTimeout=60000&useSSL=false&allowPublicKeyRetrieval=true",
                credentials);
    }

    private static void awaitSql(int port, String username, String password) {
        await().atMost(Duration.ofMinutes(3)).ignoreExceptionsMatching(failure ->
                failure instanceof SQLException sql && sql.getSQLState() != null
                        && sql.getSQLState().startsWith("08")).until(() -> {
                            try (Connection connection = connect(port, username, password)) {
                                return connection.isValid(5);
                            }
                        });
    }

    private static void assertRejectedPassword(int port, String username, String password) {
        SQLException failure = assertThrows(SQLException.class, () -> {
            try (Connection connection = connect(port, username, password)) {
                scalar(connection, "SELECT 1");
            }
        });
        assertEquals("28000", failure.getSQLState());
    }

    private static void createProbes(Connection connection) throws SQLException {
        try {
            execute(connection, "CREATE DATABASE master_privilege_probe");
            execute(connection, "CREATE USER 'probe_user'@'%' IDENTIFIED BY 'Probe123!'");
            execute(connection, "GRANT SELECT, INSERT ON master_privilege_probe.* TO 'probe_user'@'%'");
            assertEquals("2", scalar(connection,
                    "SELECT count(*) FROM information_schema.SCHEMA_PRIVILEGES "
                            + "WHERE TABLE_SCHEMA = 'master_privilege_probe' "
                            + "AND PRIVILEGE_TYPE IN ('SELECT', 'INSERT')"));
        } finally {
            execute(connection, "DROP USER IF EXISTS 'probe_user'@'%'");
            execute(connection, "DROP DATABASE IF EXISTS master_privilege_probe");
        }
        execute(connection, "CREATE TABLE probe(id integer PRIMARY KEY) ENGINE=InnoDB");
        execute(connection, "INSERT INTO probe VALUES (1)");
        execute(connection, "CREATE TABLE memory_probe(marker varchar(32)) ENGINE=MEMORY");
        execute(connection, "INSERT INTO memory_probe VALUES ('memory-survived')");
    }

    private static void execute(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private static String scalar(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet result = statement.executeQuery(sql)) {
            assertTrue(result.next());
            return result.getString(1);
        }
    }
}
