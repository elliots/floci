package io.github.hectorvent.floci.services.rds.container;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.hectorvent.floci.core.common.AwsArnUtils;
import io.github.hectorvent.floci.core.common.AwsArnUtils.Arn;
import io.github.hectorvent.floci.core.storage.AccountAwareStorageBackend;
import io.github.hectorvent.floci.core.storage.StorageFactory;
import io.github.hectorvent.floci.services.rds.model.DbCluster;
import io.github.hectorvent.floci.services.rds.model.DbInstance;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.RdsException;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Cleans only database resources created by one test, including failed partial scenarios. */
final class RdsRuntimeCleanup {
    private final AccountAwareStorageBackend<DbInstance> instances;
    private final AccountAwareStorageBackend<DbCluster> clusters;
    private final Set<String> existing = new HashSet<>();
    private final URI endpoint;

    RdsRuntimeCleanup(StorageFactory factory, URI endpoint) {
        this.endpoint = endpoint;
        instances = factory.create("rds", "rds-instances.json", new TypeReference<Map<String, DbInstance>>() {});
        clusters = factory.create("rds", "rds-clusters.json", new TypeReference<Map<String, DbCluster>>() {});
        instances.scanAllAccounts().forEach(instance -> existing.add(instance.getDbInstanceArn()));
        clusters.scanAllAccounts().forEach(cluster -> existing.add(cluster.getDbClusterArn()));
    }

    void cleanup(Throwable testFailure) {
        List<Throwable> failures = new ArrayList<>();
        for (DbInstance instance : instances.scanAllAccounts()) {
            String arn = instance.getDbInstanceArn();
            if (existing.contains(arn)) {
                continue;
            }
            try (RdsClient client = client(arn)) {
                if (instance.isDeletionProtection()) {
                    client.modifyDBInstance(request -> request.dbInstanceIdentifier(instance.getDbInstanceIdentifier())
                            .deletionProtection(false).applyImmediately(true));
                }
                client.deleteDBInstance(request -> request.dbInstanceIdentifier(instance.getDbInstanceIdentifier())
                        .skipFinalSnapshot(true));
            } catch (RuntimeException failure) {
                recordFailure(failures, failure, "DBInstanceNotFound");
            }
        }
        for (DbCluster cluster : clusters.scanAllAccounts()) {
            String arn = cluster.getDbClusterArn();
            if (existing.contains(arn)) {
                continue;
            }
            try (RdsClient client = client(arn)) {
                client.deleteDBCluster(request -> request.dbClusterIdentifier(cluster.getDbClusterIdentifier())
                        .skipFinalSnapshot(true));
            } catch (RuntimeException failure) {
                recordFailure(failures, failure, "DBClusterNotFoundFault");
            }
        }
        if (!failures.isEmpty()) {
            AssertionError cleanupFailure = new AssertionError("Could not clean up all databases created by this test");
            failures.forEach(cleanupFailure::addSuppressed);
            if (testFailure != null) {
                testFailure.addSuppressed(cleanupFailure);
            } else {
                throw cleanupFailure;
            }
        }
    }

    private static void recordFailure(List<Throwable> failures, RuntimeException failure, String missingCode) {
        if (!(failure instanceof RdsException rds) || rds.awsErrorDetails() == null
                || !missingCode.equals(rds.awsErrorDetails().errorCode())) {
            failures.add(failure);
        }
    }

    private RdsClient client(String resourceArn) {
        Arn arn = AwsArnUtils.parse(resourceArn);
        return RdsClient.builder().endpointOverride(endpoint).region(Region.of(arn.region()))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(arn.accountId(), "test")))
                .httpClientBuilder(UrlConnectionHttpClient.builder().socketTimeout(Duration.ofMinutes(4))).build();
    }
}
