package io.github.hectorvent.floci.services.rds.proxy;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * What an Aurora MySQL cluster answers that its plain MySQL backend does not. Aurora clients, such as the AWS Advanced
 * JDBC Wrapper's Aurora MySQL dialect, ask the server which instance they reached and what the cluster's topology is,
 * with an Aurora-only system variable and the {@code information_schema.replica_host_status} table. The backend has
 * neither, so the connection fails on its first query ("Unknown system variable 'aurora_server_id'").
 *
 * <p>These statements are answered the way a one-instance cluster would answer them, by rewriting the client's query to a
 * {@code SELECT} of constants that the backend can run: the cluster's writer is the only instance, and it is the one
 * connected to. Only the complete statements these clients send are answered; anything else is relayed untouched.
 */
public final class AuroraMySqlEmulation {

    private static final Pattern TOKEN = Pattern.compile("'(?:''|[^'])*'|@@[a-zA-Z_]+|[a-zA-Z_][a-zA-Z_0-9]*|[0-9]+|[.,()=;<]");
    // The statements below are a few hundred characters. A longer one is relayed without being tokenized, which also
    // keeps a long quoted literal from overflowing the stack: TOKEN recurses once per character inside quotes.
    private static final int MAX_STATEMENT_LENGTH = 1024;
    private static final String SERVER_QUERY = normalize("SELECT @@aurora_server_id");
    // The form that version 3.0 and later of the AWS Advanced JDBC Wrapper sends: the instance id and its name.
    private static final String SERVER_ID_AND_NAME_QUERY = normalize("SELECT @@aurora_server_id, @@aurora_server_id");
    private static final String VERSION_QUERY = normalize("SHOW VARIABLES LIKE 'aurora_version'");
    private static final String VERSION_FUNCTION = normalize("SELECT AURORA_VERSION()");
    private static final String WRITER_QUERY = normalize("SELECT SERVER_ID FROM information_schema.replica_host_status "
            + "WHERE SESSION_ID = 'MASTER_SESSION_ID' AND SERVER_ID = @@aurora_server_id");
    private static final String TOPOLOGY_QUERY = normalize("SELECT SERVER_ID, "
            + "CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END, CPU, "
            + "REPLICA_LAG_IN_MILLISECONDS, LAST_UPDATE_TIMESTAMP FROM information_schema.replica_host_status "
            + "WHERE time_to_sec(timediff(now(), LAST_UPDATE_TIMESTAMP)) <= 300 OR SESSION_ID = 'MASTER_SESSION_ID'");
    private static final Set<String> STATEMENTS = Set.of(SERVER_QUERY, SERVER_ID_AND_NAME_QUERY, VERSION_QUERY,
            VERSION_FUNCTION, WRITER_QUERY, TOPOLOGY_QUERY);

    /** The instance a connection to the cluster reaches, and the cluster's Aurora version (the {@code aurora_version} variable). */
    public record Server(String serverId, String auroraVersion) {}

    private AuroraMySqlEmulation() {}

    /**
     * The {@code aurora_version} an engine version names: {@code 8.0.mysql_aurora.3.04.6} is Aurora 3.04.6. A version that
     * does not follow that form is returned as it is.
     */
    public static String auroraVersion(String engineVersion) {
        if (engineVersion == null) {
            return "";
        }
        int i = engineVersion.indexOf("mysql_aurora.");
        return i < 0 ? engineVersion : engineVersion.substring(i + "mysql_aurora.".length());
    }

    /**
     * Returns a replacement COM_QUERY payload only for a complete, recognized discovery statement. {@code cluster}
     * is asked for the instance only then, so other statements cost no lookup.
     */
    static byte[] rewrite(byte[] payload, Supplier<Server> cluster) {
        if (payload.length < 2 || payload[0] != 0x03) {
            return null;
        }
        // Connector/J negotiates query attributes and sends zero parameters, one parameter set.
        // Nonempty attributes are deliberately left to the backend instead of guessing their layout.
        int offset = payload.length > 3 && payload[1] == 0 && payload[2] == 1 ? 3 : 1;
        if (payload.length - offset > MAX_STATEMENT_LENGTH) {
            return null;
        }
        String statement = normalize(new String(payload, offset, payload.length - offset, StandardCharsets.ISO_8859_1));
        if (statement == null || !STATEMENTS.contains(statement)) {
            return null;
        }
        Server server = cluster.get();
        if (server == null || server.serverId() == null) {
            return null;
        }
        String answer;
        if (statement.equals(SERVER_QUERY)) {
            answer = "SELECT " + literal(server.serverId()) + " AS `@@aurora_server_id`";
        } else if (statement.equals(SERVER_ID_AND_NAME_QUERY)) {
            answer = "SELECT " + literal(server.serverId()) + " AS `@@aurora_server_id`, "
                    + literal(server.serverId()) + " AS `@@aurora_server_id`";
        } else if (statement.equals(VERSION_QUERY)) {
            answer = "SELECT 'aurora_version' AS Variable_name, " + literal(server.auroraVersion()) + " AS Value";
        } else if (statement.equals(VERSION_FUNCTION)) {
            answer = "SELECT " + literal(server.auroraVersion()) + " AS `AURORA_VERSION()`";
        } else if (statement.equals(WRITER_QUERY)) {
            answer = "SELECT " + literal(server.serverId()) + " AS SERVER_ID";
        } else {
            answer = "SELECT " + literal(server.serverId()) + " AS SERVER_ID, TRUE AS IS_WRITER, 0.0 AS CPU, "
                    + "0.0 AS REPLICA_LAG_IN_MILLISECONDS, NOW(6) AS LAST_UPDATE_TIMESTAMP";
        }
        byte[] sql = answer.getBytes(StandardCharsets.ISO_8859_1);
        byte[] rewritten = new byte[offset + sql.length];
        System.arraycopy(payload, 0, rewritten, 0, offset);
        System.arraycopy(sql, 0, rewritten, offset, sql.length);
        return rewritten;
    }

    /** The statement's tokens, lowercased outside quotes, or {@code null} when it holds anything else, such as a comment. */
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

    private static String literal(String s) {
        return "'" + s.replace("\\", "\\\\").replace("'", "''") + "'";
    }
}
