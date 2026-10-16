package io.github.hectorvent.floci.services.rds.proxy;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Answers the Aurora discovery statements used by the AWS Advanced JDBC Wrapper against a local MySQL writer. */
public final class AuroraMySqlEmulation {
    private static final Pattern TOKEN = Pattern.compile("'(?:''|[^'])*'|@@[a-zA-Z_]+|[a-zA-Z_][a-zA-Z_0-9]*|[0-9]+|[.,()=;<]");
    private static final String SERVER_QUERY = normalize("SELECT @@aurora_server_id");
    private static final String VERSION_QUERY = normalize("SHOW VARIABLES LIKE 'aurora_version'");
    private static final String VERSION_FUNCTION = normalize("SELECT AURORA_VERSION()");
    private static final String WRITER_QUERY = normalize("SELECT SERVER_ID FROM information_schema.replica_host_status "
            + "WHERE SESSION_ID = 'MASTER_SESSION_ID' AND SERVER_ID = @@aurora_server_id");
    private static final String TOPOLOGY_QUERY = normalize("SELECT SERVER_ID, "
            + "CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END, CPU, "
            + "REPLICA_LAG_IN_MILLISECONDS, LAST_UPDATE_TIMESTAMP FROM information_schema.replica_host_status "
            + "WHERE time_to_sec(timediff(now(), LAST_UPDATE_TIMESTAMP)) <= 300 OR SESSION_ID = 'MASTER_SESSION_ID'");

    public record Server(String serverId, String auroraVersion) {}

    private AuroraMySqlEmulation() {}

    public static String auroraVersion(String engineVersion) {
        if (engineVersion == null) {
            return "";
        }
        int index = engineVersion.indexOf("mysql_aurora.");
        return index < 0 ? engineVersion : engineVersion.substring(index + "mysql_aurora.".length());
    }

    /** Returns a replacement COM_QUERY payload only for a complete, recognized discovery statement. */
    static byte[] rewrite(byte[] payload, Server server) {
        if (payload.length < 2 || payload[0] != 0x03 || server == null || server.serverId() == null) {
            return null;
        }
        // Connector/J negotiates query attributes and sends zero parameters, one parameter set.
        // Nonempty attributes are deliberately left to the backend instead of guessing their layout.
        int offset = payload.length > 3 && payload[1] == 0 && payload[2] == 1 ? 3 : 1;
        String statement = normalize(new String(payload, offset, payload.length - offset, StandardCharsets.ISO_8859_1));
        if (statement == null) {
            return null;
        }
        String answer;
        if (statement.equals(SERVER_QUERY)) {
            answer = "SELECT " + literal(server.serverId()) + " AS `@@aurora_server_id`";
        } else if (statement.equals(VERSION_QUERY)) {
            answer = "SELECT 'aurora_version' AS Variable_name, " + literal(server.auroraVersion()) + " AS Value";
        } else if (statement.equals(VERSION_FUNCTION)) {
            answer = "SELECT " + literal(server.auroraVersion()) + " AS `AURORA_VERSION()`";
        } else if (statement.equals(WRITER_QUERY)) {
            answer = "SELECT " + literal(server.serverId()) + " AS SERVER_ID";
        } else if (statement.equals(TOPOLOGY_QUERY)) {
            answer = "SELECT " + literal(server.serverId()) + " AS SERVER_ID, TRUE AS IS_WRITER, 0.0 AS CPU, "
                    + "0.0 AS REPLICA_LAG_IN_MILLISECONDS, NOW(6) AS LAST_UPDATE_TIMESTAMP";
        } else {
            return null;
        }
        byte[] sql = answer.getBytes(StandardCharsets.ISO_8859_1);
        byte[] rewritten = new byte[offset + sql.length];
        System.arraycopy(payload, 0, rewritten, 0, offset);
        System.arraycopy(sql, 0, rewritten, offset, sql.length);
        return rewritten;
    }

    private static String normalize(String sql) {
        Matcher tokens = TOKEN.matcher(sql);
        List<String> result = new ArrayList<>();
        int end = 0;
        while (tokens.find()) {
            if (!sql.substring(end, tokens.start()).isBlank()) {
                return null;
            }
            String token = tokens.group();
            result.add(token.startsWith("'") ? token : token.toLowerCase(Locale.ROOT));
            end = tokens.end();
        }
        if (!sql.substring(end).isBlank()) {
            return null;
        }
        if (!result.isEmpty() && result.getLast().equals(";")) {
            result.removeLast();
        }
        return String.join(" ", result);
    }

    private static String literal(String value) {
        return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'";
    }
}
