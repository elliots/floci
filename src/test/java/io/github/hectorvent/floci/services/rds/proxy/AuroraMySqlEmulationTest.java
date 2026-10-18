package io.github.hectorvent.floci.services.rds.proxy;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class AuroraMySqlEmulationTest {

    private static final AuroraMySqlEmulation.Server SERVER =
            new AuroraMySqlEmulation.Server("app-cluster-instance-1", "3.04.6");
    private static final Supplier<AuroraMySqlEmulation.Server> CLUSTER = () -> SERVER;

    private static byte[] query(String sql) {
        byte[] text = sql.getBytes(StandardCharsets.ISO_8859_1);
        byte[] payload = new byte[text.length + 1];
        payload[0] = 0x03;
        System.arraycopy(text, 0, payload, 1, text.length);
        return payload;
    }

    private static String sql(byte[] payload) {
        return new String(payload, 1, payload.length - 1, StandardCharsets.ISO_8859_1);
    }

    @Test
    void auroraVersionIsTheTailOfTheEngineVersion() {
        assertEquals("3.04.6", AuroraMySqlEmulation.auroraVersion("8.0.mysql_aurora.3.04.6"));
        assertEquals("8.0.36", AuroraMySqlEmulation.auroraVersion("8.0.36"));
    }

    @Test
    void theInstanceIdQueryAnswersWithTheWriter() {
        assertEquals("SELECT 'app-cluster-instance-1' AS `@@aurora_server_id`",
                sql(AuroraMySqlEmulation.rewrite(query("SELECT @@aurora_server_id"), CLUSTER)));
    }

    @Test
    void theAuroraVersionVariableIsAnswered() {
        assertEquals("SELECT 'aurora_version' AS Variable_name, '3.04.6' AS Value",
                sql(AuroraMySqlEmulation.rewrite(query("SHOW VARIABLES LIKE 'aurora_version'"), CLUSTER)));
    }

    @Test
    void theTopologyIsOneWriter() {
        String answer = sql(AuroraMySqlEmulation.rewrite(query(
                "SELECT SERVER_ID, CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END, CPU, "
                        + "REPLICA_LAG_IN_MILLISECONDS, LAST_UPDATE_TIMESTAMP FROM information_schema.replica_host_status "
                        + "WHERE time_to_sec(timediff(now(), LAST_UPDATE_TIMESTAMP)) <= 300 OR SESSION_ID = 'MASTER_SESSION_ID' "),
                CLUSTER));
        assertEquals("SELECT 'app-cluster-instance-1' AS SERVER_ID, TRUE AS IS_WRITER, 0.0 AS CPU, "
                + "0.0 AS REPLICA_LAG_IN_MILLISECONDS, NOW(6) AS LAST_UPDATE_TIMESTAMP", answer);
    }

    @Test
    void theWriterCheckAnswersWithARow() {
        assertEquals("SELECT 'app-cluster-instance-1' AS SERVER_ID", sql(AuroraMySqlEmulation.rewrite(query(
                "SELECT SERVER_ID FROM information_schema.replica_host_status "
                        + "WHERE SESSION_ID = 'MASTER_SESSION_ID' AND SERVER_ID = @@aurora_server_id"), CLUSTER)));
    }

    @Test
    void theQueryAttributeHeaderConnectorJSendsIsKept() {
        byte[] original = query("SELECT @@aurora_server_id");
        byte[] withHeader = new byte[original.length + 2];
        withHeader[0] = 0x03;
        withHeader[1] = 0x00; // parameter count
        withHeader[2] = 0x01; // parameter set count
        System.arraycopy(original, 1, withHeader, 3, original.length - 1);
        byte[] answer = AuroraMySqlEmulation.rewrite(withHeader, CLUSTER);
        assertEquals(0x03, answer[0]);
        assertEquals(0x00, answer[1]);
        assertEquals(0x01, answer[2]);
        assertEquals("SELECT 'app-cluster-instance-1' AS `@@aurora_server_id`",
                new String(answer, 3, answer.length - 3, StandardCharsets.ISO_8859_1));
    }

    @Test
    void otherStatementsAreLeftAlone() {
        assertNull(AuroraMySqlEmulation.rewrite(query("SELECT 1"), CLUSTER));
        assertNull(AuroraMySqlEmulation.rewrite(query("SELECT @@innodb_read_only"), CLUSTER));
        assertNull(AuroraMySqlEmulation.rewrite(query("SHOW VARIABLES LIKE 'version_comment'"), CLUSTER));
    }

    @Test
    void similarQueriesAndQuotedTextAreNeverRewritten() {
        for (String statement : new String[] {
                "SELECT * FROM information_schema.replica_host_status",
                "SELECT 'information_schema.replica_host_status'",
                "SELECT SERVER_ID FROM information_schema.replica_host_status WHERE SERVER_ID = 'other'",
                "DELETE FROM information_schema.replica_host_status",
                "SELECT @@aurora_server_id; DELETE FROM users",
                "SELECT 1 /* information_schema.replica_host_status */",
                "SHOW VARIABLES LIKE 'AURORA_VERSION'",
                "SELECT @@aurora_server_id AS custom_name"}) {
            assertNull(AuroraMySqlEmulation.rewrite(query(statement), CLUSTER), statement);
        }
    }

    @Test
    void keywordCaseWhitespaceAndOneTerminatorAreAccepted() {
        assertEquals("SELECT 'app-cluster-instance-1' AS `@@aurora_server_id`",
                sql(AuroraMySqlEmulation.rewrite(query("  select\n @@aurora_server_id ;  "), CLUSTER)));
        assertEquals("SELECT '3.04.6' AS `AURORA_VERSION()`",
                sql(AuroraMySqlEmulation.rewrite(query("select AURORA_VERSION ( )"), CLUSTER)));
    }

    @Test
    void aLongStatementIsRelayedWithoutBeingTokenized() {
        assertNull(AuroraMySqlEmulation.rewrite(
                query("INSERT INTO notes VALUES ('" + "x".repeat(100_000) + "')"), CLUSTER));
    }

    @Test
    void nonQueryCommandsAndMalformedAttributeHeadersAreUntouched() {
        byte[] prepare = query("SELECT @@aurora_server_id");
        prepare[0] = 0x16;
        assertNull(AuroraMySqlEmulation.rewrite(prepare, CLUSTER));
        assertNull(AuroraMySqlEmulation.rewrite(query("\0\2SELECT @@aurora_server_id"), CLUSTER));
    }

    @Test
    void aClusterWithNoInstanceYetRewritesNothing() {
        assertNull(AuroraMySqlEmulation.rewrite(query("SELECT @@aurora_server_id"), () -> null));
    }

    @Test
    void theInstanceIdAndNameQueryOfWrapperVersion3AnswersWithTheWriter() {
        assertEquals("SELECT 'app-cluster-instance-1' AS `@@aurora_server_id`, "
                        + "'app-cluster-instance-1' AS `@@aurora_server_id`",
                sql(AuroraMySqlEmulation.rewrite(query("SELECT @@aurora_server_id, @@aurora_server_id"), CLUSTER)));
    }

    @Test
    void theClusterIsLookedUpOnlyForADiscoveryStatement() {
        AtomicInteger lookups = new AtomicInteger();
        Supplier<AuroraMySqlEmulation.Server> counting = () -> {
            lookups.incrementAndGet();
            return SERVER;
        };

        assertNull(AuroraMySqlEmulation.rewrite(query("SELECT * FROM orders"), counting));
        assertEquals(0, lookups.get());
        AuroraMySqlEmulation.rewrite(query("SELECT @@aurora_server_id"), counting);
        assertEquals(1, lookups.get());
    }
}
