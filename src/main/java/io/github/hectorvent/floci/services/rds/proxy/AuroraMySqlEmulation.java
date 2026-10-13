package io.github.hectorvent.floci.services.rds.proxy;

import java.nio.charset.StandardCharsets;
import java.util.Locale;

/**
 * What an Aurora MySQL cluster answers that its plain MySQL backend does not. Clients of Aurora (the AWS Advanced JDBC
 * Wrapper's Aurora MySQL dialect, which Tyro's database library uses for every Aurora cluster endpoint) ask the server
 * which instance they reached and what the cluster's topology is, with an Aurora-only system variable and the
 * {@code information_schema.replica_host_status} table. The backend has neither, so the connection fails on its first
 * query ("Unknown system variable 'aurora_server_id'").
 *
 * <p>These statements are answered the way a one-instance cluster would answer them, by rewriting the client's query to a
 * {@code SELECT} of constants that the backend can run: the cluster's writer is the only instance, and it is the one
 * connected to. Anything else is relayed untouched.
 */
public final class AuroraMySqlEmulation {

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
     * The COM_QUERY payload (the command byte, then the SQL) to send the backend in place of {@code payload}, or {@code null}
     * when the statement is not one Aurora answers differently.
     */
    static byte[] rewrite(byte[] payload, Server server) {
        if (payload.length < 2 || server == null || server.serverId() == null) {
            return null;
        }
        String sql = new String(payload, 1, payload.length - 1, StandardCharsets.ISO_8859_1);
        String normal = sql.trim().replaceAll("\\s+", " ").replaceAll(";$", "").toLowerCase(Locale.ROOT);
        String answer;
        if (normal.contains("information_schema.replica_host_status")) {
            if (normal.contains("@@aurora_server_id")) {
                // "is this connection the writer": a row when the instance it reached is the cluster's writer
                answer = "SELECT " + literal(server.serverId()) + " AS SERVER_ID";
            } else {
                // the topology: the one instance, the writer (SESSION_ID = 'MASTER_SESSION_ID'), with no lag
                answer = "SELECT " + literal(server.serverId()) + " AS SERVER_ID, TRUE AS IS_WRITER, 0.0 AS CPU, "
                        + "0.0 AS REPLICA_LAG_IN_MILLISECONDS, NOW() AS LAST_UPDATE_TIMESTAMP";
            }
        } else if (normal.equals("select @@aurora_server_id")) {
            answer = "SELECT " + literal(server.serverId());
        } else if (normal.equals("show variables like 'aurora_version'")) {
            answer = "SELECT 'aurora_version' AS Variable_name, " + literal(server.auroraVersion()) + " AS Value";
        } else {
            return null;
        }
        // Keep what precedes the SQL as the client sent it: the command byte and, when the client negotiated query
        // attributes (Connector/J 8 does), their two-byte header (no attributes, one parameter set). Dropping it makes the
        // server read the first bytes of the statement as that header and refuse the packet as malformed.
        int skip = 0;
        while (skip < sql.length() && sql.charAt(skip) <= ' ') {
            skip++;
        }
        byte[] text = answer.getBytes(StandardCharsets.ISO_8859_1);
        byte[] out = new byte[1 + skip + text.length];
        System.arraycopy(payload, 0, out, 0, 1 + skip);
        System.arraycopy(text, 0, out, 1 + skip, text.length);
        return out;
    }

    private static String literal(String s) {
        return "'" + s.replace("\\", "\\\\").replace("'", "''") + "'";
    }
}
