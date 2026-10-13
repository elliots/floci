package io.github.hectorvent.floci.services.rds;

import io.github.hectorvent.floci.core.common.Pem;
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

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
            KmsKey key = kms.createKey("rebase cluster key", "us-east-1");
            try {
                DBCluster created = client.createDBCluster(request -> request.dbClusterIdentifier(id)
                        .engine("aurora-mysql").engineVersion("8.0.mysql_aurora.3.05.2")
                        .masterUsername("admin").masterUserPassword("test-password-123")
                        .storageEncrypted(true).kmsKeyId(key.getKeyId())).dbCluster();
                assertEquals(key.getArn(), created.kmsKeyId());
                assertEquals(key.getArn(), client.describeDBClusters(request -> request.dbClusterIdentifier(id))
                        .dbClusters().getFirst().kmsKeyId());
            } finally {
                client.deleteDBCluster(request -> request.dbClusterIdentifier(id).skipFinalSnapshot(true));
                kms.scheduleKeyDeletion(key.getKeyId(), 7, "us-east-1");
            }
        }
    }

    @Test
    void localTlsHelpersExposeCertificateAndAcceptHostnames() {
        given().contentType("application/json").body("{\"hosts\":[\"database.example.test\"]}")
                .post("/_aws/rds/tls/hosts").then().statusCode(204);
        String certificate = given().get("/_aws/rds/tls/ca").then().statusCode(200)
                .contentType("application/x-pem-file").extract().asString();
        assertNotNull(Pem.parseCertificate(certificate));
        given().contentType("application/json").body("{\"hosts\":\"invalid\"}")
                .post("/_aws/rds/tls/hosts").then().statusCode(400);
    }
}
