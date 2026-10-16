package io.github.hectorvent.floci.services.rds.container;

import com.sun.net.httpserver.HttpServer;
import io.github.hectorvent.floci.core.storage.AccountAwareStorageBackend;
import io.github.hectorvent.floci.core.storage.StorageFactory;
import io.github.hectorvent.floci.services.rds.model.DbCluster;
import io.github.hectorvent.floci.services.rds.model.DbInstance;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RdsRuntimeCleanupTest {
    @Test
    void cleansNewResourcesInTheirOwningAccountAndLeavesBaselineUntouched() throws Exception {
        exerciseCleanup(false);
    }

    @Test
    void failedMemberCleanupStillAttemptsClusterAndPreservesOriginalFailure() throws Exception {
        exerciseCleanup(true);
    }

    private static void exerciseCleanup(boolean failMember) throws Exception {
        List<Map<String, String>> requests = new CopyOnWriteArrayList<>();
        List<String> authorizations = new CopyOnWriteArrayList<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            Map<String, String> fields = Arrays.stream(new String(exchange.getRequestBody().readAllBytes(),
                    StandardCharsets.UTF_8).split("&")).map(field -> field.split("=", 2))
                    .collect(Collectors.toMap(pair -> URLDecoder.decode(pair[0], StandardCharsets.UTF_8),
                            pair -> URLDecoder.decode(pair[1], StandardCharsets.UTF_8)));
            requests.add(fields);
            authorizations.add(exchange.getRequestHeaders().getFirst("Authorization"));
            String action = fields.get("Action");
            boolean denied = failMember && action.equals("DeleteDBInstance");
            String xml = denied
                    ? "<ErrorResponse><Error><Type>Sender</Type><Code>AccessDenied</Code>"
                            + "<Message>cleanup denied</Message></Error><RequestId>test</RequestId></ErrorResponse>"
                    : "<" + action + "Response><" + action + "Result/></" + action + "Response>";
            byte[] body = xml.getBytes(StandardCharsets.UTF_8);
            try (exchange) {
                exchange.getResponseHeaders().set("Content-Type", "text/xml");
                exchange.sendResponseHeaders(denied ? 403 : 200, body.length);
                exchange.getResponseBody().write(body);
            }
        });
        server.start();
        try {
            StorageFactory factory = mock(StorageFactory.class);
            AccountAwareStorageBackend<DbInstance> instances = instanceStore(factory);
            AccountAwareStorageBackend<DbCluster> clusters = clusterStore(factory);
            List<DbInstance> instanceRecords = new ArrayList<>(List.of(instance("existing")));
            List<DbCluster> clusterRecords = new ArrayList<>(List.of(cluster("existing")));
            when(instances.scanAllAccounts()).thenAnswer(_ -> List.copyOf(instanceRecords));
            when(clusters.scanAllAccounts()).thenAnswer(_ -> List.copyOf(clusterRecords));
            RdsRuntimeCleanup cleanup = new RdsRuntimeCleanup(factory,
                    URI.create("http://127.0.0.1:" + server.getAddress().getPort()));
            instanceRecords.add(instance("created"));
            clusterRecords.add(cluster("created"));
            AssertionError original = new AssertionError("original test failure");
            cleanup.cleanup(failMember ? original : null);
            assertEquals(List.of("DeleteDBInstance", "DeleteDBCluster"),
                    requests.stream().map(request -> request.get("Action")).toList());
            assertEquals("created", requests.getFirst().get("DBInstanceIdentifier"));
            assertEquals("created", requests.getLast().get("DBClusterIdentifier"));
            assertEquals("true", requests.getFirst().get("SkipFinalSnapshot"));
            assertTrue(authorizations.stream().allMatch(value -> value.contains("Credential=222222222222/")));
            assertEquals(failMember ? 1 : 0, original.getSuppressed().length);
            if (failMember) {
                assertEquals(1, original.getSuppressed()[0].getSuppressed().length);
                assertTrue(original.getSuppressed()[0].getSuppressed()[0].getMessage().contains("cleanup denied"));
            }
        } finally {
            server.stop(0);
        }
    }

    @SuppressWarnings("unchecked")
    private static AccountAwareStorageBackend<DbInstance> instanceStore(StorageFactory factory) {
        AccountAwareStorageBackend<DbInstance> store = mock(AccountAwareStorageBackend.class);
        when(factory.<DbInstance>create(eq("rds"), eq("rds-instances.json"), any())).thenReturn(store);
        return store;
    }

    @SuppressWarnings("unchecked")
    private static AccountAwareStorageBackend<DbCluster> clusterStore(StorageFactory factory) {
        AccountAwareStorageBackend<DbCluster> store = mock(AccountAwareStorageBackend.class);
        when(factory.<DbCluster>create(eq("rds"), eq("rds-clusters.json"), any())).thenReturn(store);
        return store;
    }

    private static DbInstance instance(String id) {
        DbInstance instance = new DbInstance();
        instance.setDbInstanceIdentifier(id);
        instance.setDbInstanceArn("arn:aws:rds:us-east-1:222222222222:db:" + id);
        return instance;
    }

    private static DbCluster cluster(String id) {
        DbCluster cluster = new DbCluster();
        cluster.setDbClusterIdentifier(id);
        cluster.setDbClusterArn("arn:aws:rds:us-east-1:222222222222:cluster:" + id);
        return cluster;
    }
}
