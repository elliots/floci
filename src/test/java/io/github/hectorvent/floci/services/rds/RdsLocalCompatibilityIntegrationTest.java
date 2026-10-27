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
import software.amazon.awssdk.services.rds.model.DbClusterSnapshotNotFoundException;
import software.amazon.awssdk.services.rds.model.KmsKeyNotAccessibleException;
import software.amazon.awssdk.services.rds.model.RdsException;

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
        String id = "compat-kms-cluster";
        String parameterGroup = "compat-aurora-parameters";
        try (RdsClient client = client()) {
            assertEquals("aurora", client.describeOrderableDBInstanceOptions(request -> request
                    .engine("aurora-mysql").engineVersion("8.0.mysql_aurora.3.05.2"))
                    .orderableDBInstanceOptions().getFirst().storageType());
            assertThrows(KmsKeyNotAccessibleException.class, () -> client.createDBCluster(request -> request
                    .dbClusterIdentifier(id).engine("aurora-mysql").masterUsername("admin")
                    .masterUserPassword("test-password-123").storageEncrypted(true).kmsKeyId("missing-key")));
            assertThrows(DbClusterNotFoundException.class,
                    () -> client.describeDBClusters(request -> request.dbClusterIdentifier(id)));
            KmsKey key = kms.createKey("compat cluster key", "us-east-1");
            try {
                client.createDBClusterParameterGroup(request -> request.dbClusterParameterGroupName(parameterGroup)
                        .dbParameterGroupFamily("aurora-mysql8.0").description("SDK metadata validation"));
                RdsException unencrypted = assertThrows(RdsException.class,
                        () -> client.createDBCluster(request -> request.dbClusterIdentifier(id).engine("aurora-mysql")
                                .masterUsername("admin").masterUserPassword("test-password-123")
                                .kmsKeyId(key.getKeyId())));
                assertEquals("InvalidParameterCombination", unencrypted.awsErrorDetails().errorCode());
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
                assertEquals(key.getArn(), described.kmsKeyId());
                assertEquals(parameterGroup, described.dbClusterParameterGroup());
            } finally {
                client.deleteDBInstance(request -> request.dbInstanceIdentifier(id + "-writer").skipFinalSnapshot(true));
                client.deleteDBCluster(request -> request.dbClusterIdentifier(id).skipFinalSnapshot(true));
                client.deleteDBClusterParameterGroup(request -> request.dbClusterParameterGroupName(parameterGroup));
                kms.scheduleKeyDeletion(key.getKeyId(), 7, "us-east-1");
            }
        }
    }

    @Test
    void clusterWithoutParameterGroupReportsTheEngineDefault() {
        String postgres = "compat-default-group-postgres";
        String mysql = "compat-default-group-mysql";
        try (RdsClient client = client()) {
            client.createDBCluster(request -> request.dbClusterIdentifier(postgres).engine("aurora-postgresql")
                    .engineVersion("16.3").masterUsername("admin").masterUserPassword("test-password-123"));
            client.createDBCluster(request -> request.dbClusterIdentifier(mysql).engine("aurora-mysql")
                    .engineVersion("8.0.mysql_aurora.3.05.2").masterUsername("admin")
                    .masterUserPassword("test-password-123"));
            try {
                assertEquals("default.aurora-postgresql16", client.describeDBClusters(request -> request
                        .dbClusterIdentifier(postgres)).dbClusters().getFirst().dbClusterParameterGroup());
                assertEquals("default.aurora-mysql8.0", client.describeDBClusters(request -> request
                        .dbClusterIdentifier(mysql)).dbClusters().getFirst().dbClusterParameterGroup());
            } finally {
                client.deleteDBCluster(request -> request.dbClusterIdentifier(postgres).skipFinalSnapshot(true));
                client.deleteDBCluster(request -> request.dbClusterIdentifier(mysql).skipFinalSnapshot(true));
            }
        }
    }

    @Test
    void encryptedClusterSnapshotsAndRestoresKeepTheKmsIdentity() {
        String source = "compat-encrypted-source";
        String snapshot = "compat-encrypted-snapshot";
        String copy = "compat-encrypted-snapshot-copy";
        String restored = "compat-encrypted-restored";
        String pointInTime = "compat-encrypted-pitr";
        try (RdsClient client = client()) {
            KmsKey key = kms.createKey("cluster snapshot identity", "us-east-1");
            client.createDBCluster(request -> request.dbClusterIdentifier(source).engine("aurora-postgresql")
                    .masterUsername("admin").masterUserPassword("test-password-123")
                    .storageEncrypted(true).kmsKeyId(key.getKeyId()));
            try {
                assertEquals(key.getArn(), client.createDBClusterSnapshot(request -> request.dbClusterIdentifier(source)
                        .dbClusterSnapshotIdentifier(snapshot)).dbClusterSnapshot().kmsKeyId());
                assertEquals(key.getArn(), client.copyDBClusterSnapshot(request -> request
                        .sourceDBClusterSnapshotIdentifier(snapshot).targetDBClusterSnapshotIdentifier(copy))
                        .dbClusterSnapshot().kmsKeyId());
                assertEquals(key.getArn(), client.restoreDBClusterFromSnapshot(request -> request
                        .dbClusterIdentifier(restored).snapshotIdentifier(snapshot).engine("aurora-postgresql"))
                        .dbCluster().kmsKeyId());
                assertEquals(key.getArn(), client.restoreDBClusterToPointInTime(request -> request
                        .dbClusterIdentifier(pointInTime).sourceDBClusterIdentifier(source).useLatestRestorableTime(true))
                        .dbCluster().kmsKeyId());
                for (String id : new String[] {restored, pointInTime}) {
                    DBCluster described = client.describeDBClusters(request -> request.dbClusterIdentifier(id))
                            .dbClusters().getFirst();
                    assertTrue(described.storageEncrypted(), id);
                    assertEquals(key.getArn(), described.kmsKeyId(), id);
                }
            } finally {
                for (String id : new String[] {restored, pointInTime, source}) {
                    try {
                        client.deleteDBCluster(request -> request.dbClusterIdentifier(id).skipFinalSnapshot(true));
                    } catch (DbClusterNotFoundException ignored) {
                        // A failed assertion may leave a restore that was never created.
                    }
                }
                deleteSnapshots(client, copy, snapshot);
                kms.scheduleKeyDeletion(key.getKeyId(), 7, "us-east-1");
            }
        }
    }

    @Test
    void aRequestedKmsKeyEncryptsCopiesAndRestores() {
        String source = "compat-rekey-source";
        String plain = "compat-rekey-plain";
        String snapshot = "compat-rekey-snapshot";
        String plainSnapshot = "compat-rekey-plain-snapshot";
        String copy = "compat-rekey-copy";
        String restored = "compat-rekey-restored";
        String pointInTime = "compat-rekey-pitr";
        String plainRestored = "compat-rekey-plain-restored";
        try (RdsClient client = client()) {
            KmsKey original = kms.createKey("compat original key", "us-east-1");
            KmsKey requested = kms.createKey("compat requested key", "us-east-1");
            try {
                client.createDBCluster(request -> request.dbClusterIdentifier(source).engine("aurora-postgresql")
                        .masterUsername("admin").masterUserPassword("test-password-123")
                        .storageEncrypted(true).kmsKeyId(original.getKeyId()));
                client.createDBCluster(request -> request.dbClusterIdentifier(plain).engine("aurora-postgresql")
                        .masterUsername("admin").masterUserPassword("test-password-123"));
                client.createDBClusterSnapshot(request -> request.dbClusterIdentifier(source)
                        .dbClusterSnapshotIdentifier(snapshot));
                client.createDBClusterSnapshot(request -> request.dbClusterIdentifier(plain)
                        .dbClusterSnapshotIdentifier(plainSnapshot));

                assertEquals(requested.getArn(), client.copyDBClusterSnapshot(request -> request
                        .sourceDBClusterSnapshotIdentifier(snapshot).targetDBClusterSnapshotIdentifier(copy)
                        .kmsKeyId(requested.getKeyId())).dbClusterSnapshot().kmsKeyId());
                assertEquals(requested.getArn(), client.restoreDBClusterFromSnapshot(request -> request
                        .dbClusterIdentifier(restored).snapshotIdentifier(snapshot).engine("aurora-postgresql")
                        .kmsKeyId(requested.getKeyId())).dbCluster().kmsKeyId());
                assertEquals(requested.getArn(), client.restoreDBClusterToPointInTime(request -> request
                        .dbClusterIdentifier(pointInTime).sourceDBClusterIdentifier(source)
                        .useLatestRestorableTime(true).kmsKeyId(requested.getKeyId())).dbCluster().kmsKeyId());
                DBCluster encrypted = client.restoreDBClusterFromSnapshot(request -> request
                        .dbClusterIdentifier(plainRestored).snapshotIdentifier(plainSnapshot)
                        .engine("aurora-postgresql").kmsKeyId(requested.getKeyId())).dbCluster();
                assertTrue(encrypted.storageEncrypted());
                assertEquals(requested.getArn(), encrypted.kmsKeyId());

                RdsException copyOfPlain = assertThrows(RdsException.class,
                        () -> client.copyDBClusterSnapshot(request -> request
                                .sourceDBClusterSnapshotIdentifier(plainSnapshot)
                                .targetDBClusterSnapshotIdentifier("compat-rekey-refused")
                                .kmsKeyId(requested.getKeyId())));
                assertEquals("InvalidParameterCombination", copyOfPlain.awsErrorDetails().errorCode());
                RdsException restoreOfPlain = assertThrows(RdsException.class,
                        () -> client.restoreDBClusterToPointInTime(request -> request
                                .dbClusterIdentifier("compat-rekey-refused").sourceDBClusterIdentifier(plain)
                                .useLatestRestorableTime(true).kmsKeyId(requested.getKeyId())));
                assertEquals("InvalidParameterCombination", restoreOfPlain.awsErrorDetails().errorCode());
            } finally {
                for (String id : new String[] {restored, pointInTime, plainRestored, source, plain}) {
                    try {
                        client.deleteDBCluster(request -> request.dbClusterIdentifier(id).skipFinalSnapshot(true));
                    } catch (DbClusterNotFoundException ignored) {
                        // A failed assertion may leave a cluster that was never created.
                    }
                }
                deleteSnapshots(client, copy, snapshot, plainSnapshot);
                kms.scheduleKeyDeletion(original.getKeyId(), 7, "us-east-1");
                kms.scheduleKeyDeletion(requested.getKeyId(), 7, "us-east-1");
            }
        }
    }

    private static void deleteSnapshots(RdsClient client, String... ids) {
        for (String id : ids) {
            try {
                client.deleteDBClusterSnapshot(request -> request.dbClusterSnapshotIdentifier(id));
            } catch (DbClusterSnapshotNotFoundException ignored) {
                // A failed assertion may leave a snapshot that was never created.
            }
        }
    }

    private RdsClient client() {
        return RdsClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .build();
    }
}
