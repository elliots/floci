package io.github.hectorvent.floci.services.rds.proxy;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class AuroraMySqlEmulationTest {

    private static final AuroraMySqlEmulation.Server SERVER =
            new AuroraMySqlEmulation.Server("customer-zero-3-04-a-1", "3.04.6");

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
        assertEquals("SELECT 'customer-zero-3-04-a-1'",
                sql(AuroraMySqlEmulation.rewrite(query("SELECT @@aurora_server_id"), SERVER)));
    }

    @Test
    void theAuroraVersionVariableIsAnswered() {
        assertEquals("SELECT 'aurora_version' AS Variable_name, '3.04.6' AS Value",
                sql(AuroraMySqlEmulation.rewrite(query("SHOW VARIABLES LIKE 'aurora_version'"), SERVER)));
    }

    @Test
    void theTopologyIsOneWriter() {
        String answer = sql(AuroraMySqlEmulation.rewrite(query(
                "SELECT SERVER_ID, CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END, CPU, "
                        + "REPLICA_LAG_IN_MILLISECONDS, LAST_UPDATE_TIMESTAMP FROM information_schema.replica_host_status "
                        + "WHERE time_to_sec(timediff(now(), LAST_UPDATE_TIMESTAMP)) <= 300 OR SESSION_ID = 'MASTER_SESSION_ID' "),
                SERVER));
        assertEquals("SELECT 'customer-zero-3-04-a-1' AS SERVER_ID, TRUE AS IS_WRITER, 0.0 AS CPU, "
                + "0.0 AS REPLICA_LAG_IN_MILLISECONDS, NOW() AS LAST_UPDATE_TIMESTAMP", answer);
    }

    @Test
    void theWriterCheckAnswersWithARow() {
        assertEquals("SELECT 'customer-zero-3-04-a-1' AS SERVER_ID", sql(AuroraMySqlEmulation.rewrite(query(
                "SELECT SERVER_ID FROM information_schema.replica_host_status "
                        + "WHERE SESSION_ID = 'MASTER_SESSION_ID' AND SERVER_ID = @@aurora_server_id"), SERVER)));
    }

    @Test
    void theQueryAttributeHeaderConnectorJSendsIsKept() {
        byte[] original = query("SELECT @@aurora_server_id");
        byte[] withHeader = new byte[original.length + 2];
        withHeader[0] = 0x03;
        withHeader[1] = 0x00; // parameter count
        withHeader[2] = 0x01; // parameter set count
        System.arraycopy(original, 1, withHeader, 3, original.length - 1);
        byte[] answer = AuroraMySqlEmulation.rewrite(withHeader, SERVER);
        assertEquals(0x03, answer[0]);
        assertEquals(0x00, answer[1]);
        assertEquals(0x01, answer[2]);
        assertEquals("SELECT 'customer-zero-3-04-a-1'", new String(answer, 3, answer.length - 3, StandardCharsets.ISO_8859_1));
    }

    @Test
    void otherStatementsAreLeftAlone() {
        assertNull(AuroraMySqlEmulation.rewrite(query("SELECT 1"), SERVER));
        assertNull(AuroraMySqlEmulation.rewrite(query("SELECT @@innodb_read_only"), SERVER));
        assertNull(AuroraMySqlEmulation.rewrite(query("SHOW VARIABLES LIKE 'version_comment'"), SERVER));
    }

    @Test
    void aClusterWithNoInstanceYetRewritesNothing() {
        assertNull(AuroraMySqlEmulation.rewrite(query("SELECT @@aurora_server_id"), null));
    }
}
