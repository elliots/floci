package io.github.hectorvent.floci.services.rds;

import io.github.hectorvent.floci.services.kms.KmsService;
import io.github.hectorvent.floci.services.kms.model.KmsKey;
import io.github.hectorvent.floci.testing.RdsMockProfile;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DbClusterNotFoundException;
import software.amazon.awssdk.services.rds.model.KmsKeyNotAccessibleException;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@TestProfile(RdsMockProfile.class)
class RdsLocalCompatibilityIntegrationTest {
    @TestHTTPResource
    URI endpoint;
    @Inject
    KmsService kms;

    @Test
    void clusterKmsKeyRoundTripsThroughSdk() {
        String id = "rebase-kms-cluster";
        try (RdsClient client = RdsClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .build()) {
            assertEquals("aurora", client.describeOrderableDBInstanceOptions(request -> request
                    .engine("aurora-mysql").engineVersion("8.0.mysql_aurora.3.05.2"))
                    .orderableDBInstanceOptions().getFirst().storageType());
            assertThrows(KmsKeyNotAccessibleException.class, () -> client.createDBCluster(request -> request
                    .dbClusterIdentifier(id).engine("aurora-mysql").masterUsername("admin")
                    .masterUserPassword("test-password-123").storageEncrypted(true).kmsKeyId("missing-key")));
            assertThrows(DbClusterNotFoundException.class,
                    () -> client.describeDBClusters(request -> request.dbClusterIdentifier(id)));
            String parameterGroup = "compat-aurora-parameters";
            client.createDBClusterParameterGroup(request -> request.dbClusterParameterGroupName(parameterGroup)
                    .dbParameterGroupFamily("aurora-mysql8.0").description("SDK metadata validation"));
            KmsKey key = kms.createKey("rebase cluster key", "us-east-1");
            try {
                DBCluster created = client.createDBCluster(request -> request.dbClusterIdentifier(id)
                        .engine("aurora-mysql").engineVersion("8.0.mysql_aurora.3.05.2")
                        .masterUsername("admin").masterUserPassword("test-password-123")
                        .storageEncrypted(true).kmsKeyId(key.getKeyId()).enableHttpEndpoint(true)
                        .dbClusterParameterGroupName(parameterGroup)).dbCluster();
                assertEquals(key.getArn(), created.kmsKeyId());
                assertEquals(parameterGroup, created.dbClusterParameterGroup());
                assertTrue(created.httpEndpointEnabled());
                client.createDBInstance(request -> request.dbInstanceIdentifier(id + "-writer")
                        .dbClusterIdentifier(id).engine("aurora-mysql").dbInstanceClass("db.t3.medium"));
                DBCluster described = client.describeDBClusters(request -> request.dbClusterIdentifier(id))
                        .dbClusters().getFirst();
                assertEquals(1, described.dbClusterMembers().size());
                assertEquals(id + "-writer", described.dbClusterMembers().getFirst().dbInstanceIdentifier());
                assertTrue(described.dbClusterMembers().getFirst().isClusterWriter());
                assertEquals(key.getArn(), client.describeDBClusters(request -> request.dbClusterIdentifier(id))
                        .dbClusters().getFirst().kmsKeyId());
            } finally {
                client.deleteDBInstance(request -> request.dbInstanceIdentifier(id + "-writer").skipFinalSnapshot(true));
                client.deleteDBCluster(request -> request.dbClusterIdentifier(id).skipFinalSnapshot(true));
                client.deleteDBClusterParameterGroup(request -> request.dbClusterParameterGroupName(parameterGroup));
                kms.scheduleKeyDeletion(key.getKeyId(), 7, "us-east-1");
            }
        }
    }

    @Test
    void encryptedClusterSnapshotsAndRestoresKeepTheKmsIdentity() {
        String source = "compat-encrypted-source";
        String snapshot = "compat-encrypted-snapshot";
        String restored = "compat-encrypted-restored";
        String pointInTime = "compat-encrypted-pitr";
        try (RdsClient client = RdsClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .build()) {
            KmsKey key = kms.createKey("cluster snapshot identity", "us-east-1");
            client.createDBCluster(request -> request.dbClusterIdentifier(source).engine("aurora-postgresql")
                    .masterUsername("admin").masterUserPassword("test-password-123")
                    .storageEncrypted(true).kmsKeyId(key.getKeyId()));
            try {
                assertEquals(key.getArn(), client.createDBClusterSnapshot(request -> request.dbClusterIdentifier(source)
                        .dbClusterSnapshotIdentifier(snapshot)).dbClusterSnapshot().kmsKeyId());
                assertEquals(key.getArn(), client.restoreDBClusterFromSnapshot(request -> request
                        .dbClusterIdentifier(restored).snapshotIdentifier(snapshot).engine("aurora-postgresql"))
                        .dbCluster().kmsKeyId());
                assertEquals(key.getArn(), client.restoreDBClusterToPointInTime(request -> request
                        .dbClusterIdentifier(pointInTime).sourceDBClusterIdentifier(source).useLatestRestorableTime(true))
                        .dbCluster().kmsKeyId());
            } finally {
                for (String id : new String[] {restored, pointInTime, source}) {
                    try {
                        client.deleteDBCluster(request -> request.dbClusterIdentifier(id).skipFinalSnapshot(true));
                    } catch (DbClusterNotFoundException ignored) {
                        // A failed assertion may leave a restore that was never created.
                    }
                }
                client.deleteDBClusterSnapshot(request -> request.dbClusterSnapshotIdentifier(snapshot));
                kms.scheduleKeyDeletion(key.getKeyId(), 7, "us-east-1");
            }
        }
    }

}
