package io.github.hectorvent.floci.services.eks;

import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.eks.EksClient;
import software.amazon.awssdk.services.eks.model.ClusterStatus;
import software.amazon.awssdk.services.eks.model.InvalidParameterException;
import software.amazon.awssdk.services.eks.model.LogSetup;
import software.amazon.awssdk.services.eks.model.LogType;
import software.amazon.awssdk.services.eks.model.Logging;
import software.amazon.awssdk.services.eks.model.Update;
import software.amazon.awssdk.services.eks.model.UpdateParamType;
import software.amazon.awssdk.services.eks.model.UpdateStatus;
import software.amazon.awssdk.services.eks.model.UpdateType;

import java.net.URI;
import java.util.List;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** UpdateClusterConfig changes a cluster's logging, as the SDK sends it, and DescribeUpdate finds the update. */
@QuarkusTest
class EksUpdateClusterConfigIntegrationTest {

    private static final String CLUSTER = "it-update-config-cluster";

    @TestHTTPResource
    URI endpoint;

    @Test
    void updateClusterConfigTurnsLogTypesOnAndOffAndKeepsTheOthers() {
        try (EksClient eks = EksClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .build()) {
            eks.createCluster(request -> request.name(CLUSTER).roleArn("arn:aws:iam::000000000000:role/eks")
                    .logging(logging(true, LogType.AUDIT)));
            await().until(() -> eks.describeCluster(r -> r.name(CLUSTER)).cluster().status() == ClusterStatus.ACTIVE);

            Update update = eks.updateClusterConfig(request -> request.name(CLUSTER)
                    .logging(Logging.builder().clusterLogging(LogSetup.builder().types(LogType.API).enabled(true)
                            .build()).build())).update();

            assertEquals(UpdateType.LOGGING_UPDATE, update.type());
            assertEquals(UpdateStatus.SUCCESSFUL, update.status());
            assertEquals(UpdateParamType.CLUSTER_LOGGING, update.params().getFirst().type());
            assertThat(update.params().getFirst().value(), containsString("\"api\""));
            assertEquals(update.id(), eks.describeUpdate(r -> r.name(CLUSTER).updateId(update.id())).update().id());
            LogSetup enabled = eks.describeCluster(r -> r.name(CLUSTER)).cluster().logging().clusterLogging()
                    .getFirst();
            assertEquals(true, enabled.enabled());
            assertThat(enabled.types(), containsInAnyOrder(LogType.API, LogType.AUDIT));

            InvalidParameterException noChange = assertThrows(InvalidParameterException.class,
                    () -> eks.updateClusterConfig(request -> request.name(CLUSTER).logging(logging(true, LogType.API))));
            assertThat(noChange.getMessage(), containsString("No changes needed"));
            assertThrows(InvalidParameterException.class, () -> eks.updateClusterConfig(request -> request
                    .name(CLUSTER).resourcesVpcConfig(vpc -> vpc.endpointPublicAccess(false))));

            eks.updateClusterConfig(request -> request.name(CLUSTER).logging(logging(false, LogType.API,
                    LogType.AUDIT)));
            assertEquals(false, eks.describeCluster(r -> r.name(CLUSTER)).cluster().logging().clusterLogging()
                    .getFirst().enabled());
            eks.deleteCluster(r -> r.name(CLUSTER));
        }
    }

    private static Logging logging(boolean enabled, LogType... types) {
        return Logging.builder().clusterLogging(LogSetup.builder().types(List.of(types)).enabled(enabled).build())
                .build();
    }
}
