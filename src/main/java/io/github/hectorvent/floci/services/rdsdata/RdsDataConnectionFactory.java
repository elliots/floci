package io.github.hectorvent.floci.services.rdsdata;

import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager.EndpointInfo;
import io.github.hectorvent.floci.runtime.node.TunnelRelay;
import io.github.hectorvent.floci.services.rds.container.RdsContainerManager;
import io.github.hectorvent.floci.services.rds.model.DatabaseEngine;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Properties;

/**
 * JDBC connections to the database of an RDS resource. A database on the smolvm-pods node is
 * reached through the node's tunnel, as RDS's proxies reach it, which wakes a sleeping database:
 * the driver connects to a {@link TunnelRelay} on {@code 127.0.0.1} that carries its one
 * connection through the tunnel.
 */
@ApplicationScoped
class RdsDataConnectionFactory {

    /** How long a relay waits for the driver to connect to it. */
    static final Duration RELAY_ACCEPT_TIMEOUT = Duration.ofSeconds(10);
    /** How long the connection to the node service may take; the database's wake comes after. */
    private static final int NODE_CONNECT_TIMEOUT_MILLIS = 5000;

    static {
        // quarkus-jdbc-mysql set this at startup in JVM mode. Connector/J reads it once, in
        // the static initializer that otherwise starts its abandoned connection cleanup thread.
        System.setProperty("com.mysql.cj.disableAbandonedConnectionCleanup", "true");
    }

    /** Where the databases run, or null for direct connections only, as in tests without CDI. */
    private final RdsContainerManager containerManager;

    RdsDataConnectionFactory() {
        this(null);
    }

    @Inject
    RdsDataConnectionFactory(RdsContainerManager containerManager) {
        this.containerManager = containerManager;
    }

    Connection open(RdsDataResourceResolver.DatabaseTarget target,
                    EndpointInfo endpoint,
                    String username,
                    String password,
                    String database) throws SQLException {
        if (containerManager == null || !containerManager.onSmolvmNode()) {
            return connect(target, endpoint.host(), endpoint.port(), username, password, database);
        }
        TunnelRelay relay;
        try {
            relay = TunnelRelay.open(target.arn(), RELAY_ACCEPT_TIMEOUT, () -> containerManager.connectOnNode(
                    endpoint.host(), endpoint.port(), NODE_CONNECT_TIMEOUT_MILLIS));
        } catch (IOException e) {
            throw new SQLException("Could not open a tunnel to the database of " + target.arn() + ": "
                    + e.getMessage(), "08001", e);
        }
        try {
            return connect(target, TunnelRelay.HOST, relay.port(), username, password, database);
        } catch (SQLException e) {
            relay.close();
            Exception failure = relay.failure();
            if (failure != null) {
                throw new SQLException("Could not open a tunnel to the database of " + target.arn() + ": "
                        + failure.getMessage(), "08001", failure);
            }
            throw e;
        }
    }

    private static Connection connect(RdsDataResourceResolver.DatabaseTarget target, String host, int port,
                                      String username, String password, String database) throws SQLException {
        String url = buildUrl(target.engine(), host, port, database);
        Properties props = new Properties();
        props.setProperty("user", username);
        props.setProperty("password", password);
        props.setProperty("connectTimeout", connectTimeout(target.engine()));
        return DriverManager.getConnection(url, props);
    }

    static String buildUrl(DatabaseEngine engine, String host, int port, String database) {
        return switch (engine) {
            case MYSQL, MARIADB -> "jdbc:mysql://" + host + ":" + port + "/" + database
                    + "?useSSL=false&allowPublicKeyRetrieval=true";
            case POSTGRES -> "jdbc:postgresql://" + host + ":" + port + "/" + database
                    + "?sslmode=disable";
            case SQLSERVER -> throw new IllegalArgumentException(
                    "RDS Data API is not supported for SQL Server");
        };
    }

    private static String connectTimeout(DatabaseEngine engine) {
        // MySQL Connector/J expects milliseconds; the PostgreSQL driver expects seconds.
        return switch (engine) {
            case MYSQL, MARIADB -> "5000";
            case POSTGRES -> "5";
            case SQLSERVER -> "5000";
        };
    }
}
