package io.github.hectorvent.floci.services.rds;

import io.github.hectorvent.floci.testing.RdsMockProfile;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DbClusterNotFoundException;
import software.amazon.awssdk.services.rds.model.RdsException;

import java.net.URI;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@QuarkusTest
@TestProfile(RdsMockProfile.class)
class RdsServerlessV2VersionIntegrationTest {
    @TestHTTPResource
    URI endpoint;

    @ParameterizedTest
    @CsvSource({
            "aurora-postgresql,13.14,false", "aurora-postgresql,13.15,true",
            "aurora-postgresql,14.11,false", "aurora-postgresql,14.12,true",
            "aurora-postgresql,15.6,false", "aurora-postgresql,15.7,true",
            "aurora-postgresql,16.2,false", "aurora-postgresql,16.3,true",
            "aurora-postgresql,17.4,true",
            "aurora-mysql,8.0.mysql_aurora.3.07.1,false", "aurora-mysql,8.0.mysql_aurora.3.08.0,true"
    })
    void zeroMinimumUsesEffectiveVersionOnCreateAndModify(String engine, String version, boolean supported) {
        String id = "version-" + UUID.randomUUID().toString().substring(0, 8);
        try (RdsClient client = client()) {
            if (!supported) {
                RdsException failure = assertThrows(RdsException.class, () -> client.createDBCluster(request -> request
                        .dbClusterIdentifier(id).engine(engine).engineVersion(version)
                        .masterUsername("admin").masterUserPassword("test-password-123")
                        .serverlessV2ScalingConfiguration(scaling -> scaling.minCapacity(0.0).maxCapacity(2.0))));
                assertEquals("InvalidParameterValue", failure.awsErrorDetails().errorCode());
                assertThrows(DbClusterNotFoundException.class,
                        () -> client.describeDBClusters(request -> request.dbClusterIdentifier(id)));
            }
            client.createDBCluster(request -> request.dbClusterIdentifier(id).engine(engine).engineVersion(version)
                    .masterUsername("admin").masterUserPassword("test-password-123")
                    .serverlessV2ScalingConfiguration(scaling -> scaling
                            .minCapacity(supported ? 0.0 : 0.5).maxCapacity(2.0)));
            try {
                if (supported) {
                    DBCluster changed = client.modifyDBCluster(request -> request.dbClusterIdentifier(id)
                            .serverlessV2ScalingConfiguration(scaling -> scaling.secondsUntilAutoPause(600))).dbCluster();
                    assertEquals(0.0, changed.serverlessV2ScalingConfiguration().minCapacity());
                    assertEquals(600, changed.serverlessV2ScalingConfiguration().secondsUntilAutoPause());
                } else {
                    RdsException failure = assertThrows(RdsException.class, () -> client.modifyDBCluster(request -> request
                            .dbClusterIdentifier(id).serverlessV2ScalingConfiguration(scaling -> scaling.minCapacity(0.0))));
                    assertEquals("InvalidParameterValue", failure.awsErrorDetails().errorCode());
                    DBCluster unchanged = client.describeDBClusters(request -> request.dbClusterIdentifier(id))
                            .dbClusters().getFirst();
                    assertEquals(version, unchanged.engineVersion());
                    assertEquals(0.5, unchanged.serverlessV2ScalingConfiguration().minCapacity());
                }
            } finally {
                client.deleteDBCluster(request -> request.dbClusterIdentifier(id).skipFinalSnapshot(true));
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"aurora-postgresql,,16.3", "aurora-postgresql,16,16.3",
            "aurora-mysql,,8.0.mysql_aurora.3.08.0", "aurora-mysql,8.0,8.0.mysql_aurora.3.08.0"})
    void defaultsAndMajorAliasesRecordAnEligibleConcreteVersion(String engine, String version, String expected) {
        String id = "default-zero-" + UUID.randomUUID().toString().substring(0, 8);
        try (RdsClient client = client()) {
            DBCluster created = client.createDBCluster(request -> request.dbClusterIdentifier(id)
                    .engine(engine).engineVersion(version).masterUsername("admin").masterUserPassword("test-password-123")
                    .serverlessV2ScalingConfiguration(scaling -> scaling.minCapacity(0.0).maxCapacity(2.0))).dbCluster();
            try {
                assertEquals(expected, created.engineVersion());
                assertEquals(300, created.serverlessV2ScalingConfiguration().secondsUntilAutoPause());
            } finally {
                client.deleteDBCluster(request -> request.dbClusterIdentifier(id).skipFinalSnapshot(true));
            }
        }
    }

    private RdsClient client() {
        return RdsClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .build();
    }
}
