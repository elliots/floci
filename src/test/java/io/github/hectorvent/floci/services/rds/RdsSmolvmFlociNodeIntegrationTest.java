package io.github.hectorvent.floci.services.rds;

import io.github.hectorvent.floci.runtime.flocinode.FakeFlociNodes;
import io.github.hectorvent.floci.runtime.flocinode.FakeNodeProfile;
import io.github.hectorvent.floci.runtime.flocinode.FlociNode;
import io.github.hectorvent.floci.runtime.node.FakeDockerNode;
import io.github.hectorvent.floci.services.rds.model.DbInstance;
import io.github.hectorvent.floci.testing.CfnStackWaits;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.eks.EksClient;
import software.amazon.awssdk.services.eks.model.ResourceLimitExceededException;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DBInstance;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.github.hectorvent.floci.testing.RdsRequests.rdsClient;
import static io.restassured.RestAssured.given;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RDS and EKS on the one smolvm-pods node that Floci creates, on fakes ({@link FakeNodeProfile}):
 * the first database has Floci create the node and stays {@code creating} until the node is
 * ready; an EKS cluster created after it adopts the node, and a second is refused; the Data API
 * reaches the database through the node's tunnel; and deleting the cluster and the database keeps
 * the node.
 */
@QuarkusTest
@TestProfile(RdsSmolvmFlociNodeIntegrationTest.FlociNodeProfile.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RdsSmolvmFlociNodeIntegrationTest {

    private static final String ACCOUNT = "000000000000";
    private static final String DATABASE = "node-pg";
    private static final String DB_CLUSTER = "node-aurora";
    private static final String CLUSTER = "node-eks";
    private static final String ROLE = "arn:aws:iam::" + ACCOUNT + ":role/eks-cluster";
    private static final String CFN_AUTH =
            "AWS4-HMAC-SHA256 Credential=test/20260205/us-east-1/cloudformation/aws4_request";

    /** RDS and EKS on the node that Floci creates, whose fake node service runs a dockerd. */
    public static class FlociNodeProfile extends FakeNodeProfile {

        @Override
        public Map<String, String> getConfigOverrides() {
            Map<String, String> overrides = FakeNodeProfile.overrides();
            overrides.put(FakeFlociNodes.SERVICE_PROPERTY, "docker");
            overrides.put("floci.services.rds.smolvm.enabled", "true");
            overrides.put("floci.services.eks.mock", "false");
            overrides.put("floci.services.eks.provider", "smolvm-pods");
            return overrides;
        }
    }

    @TestHTTPResource
    URI endpoint;

    @Inject
    FakeFlociNodes driver;

    @Inject
    FlociNode flociNode;

    @Inject
    RdsService rdsService;

    private RdsClient rds;
    private EksClient eks;

    @BeforeEach
    void clients() {
        if (rds != null) {
            return;
        }
        given().baseUri(endpoint.toString()).when().post("/_floci/state/reset").then().statusCode(200);
        driver.reset();
        rds = rdsClient("us-east-1", ACCOUNT);
        eks = EksClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .httpClientBuilder(UrlConnectionHttpClient.builder()).build();
    }

    @AfterAll
    void closeClients() {
        rds.close();
        eks.close();
    }

    private String instanceStatus(String id) {
        return rds.describeDBInstances(request -> request.dbInstanceIdentifier(id)).dbInstances().getFirst()
                .dbInstanceStatus();
    }

    private String clusterStatus(String name) {
        return eks.describeCluster(request -> request.name(name)).cluster().statusAsString();
    }

    @Test
    @Order(1)
    void theFirstDatabaseHasFlociCreateTheNodeAndIsCreatingUntilTheNodeIsReady() {
        CountDownLatch create = driver.holdCreates();

        DBInstance created = rds.createDBInstance(request -> request.dbInstanceIdentifier(DATABASE)
                .engine("postgres").masterUsername("admin").manageMasterUserPassword(true).dbName("appdb")
                .dbInstanceClass("db.t3.micro").allocatedStorage(20)).dbInstance();

        assertEquals("creating", created.dbInstanceStatus());
        await().during(1, TimeUnit.SECONDS).atMost(3, TimeUnit.SECONDS)
                .until(() -> instanceStatus(DATABASE).equals("creating"));
        assertEquals(FlociNode.State.CREATING, flociNode.status().state());
        assertEquals(1, driver.calls("create").size());

        create.countDown();

        await().atMost(30, TimeUnit.SECONDS).pollInterval(Duration.ofMillis(250))
                .until(() -> instanceStatus(DATABASE).equals("available"));
        DbInstance instance = rdsService.getDbInstance(DATABASE);
        FakeDockerNode node = driver.dockerNode();
        assertTrue(node.containers().containsKey(instance.getContainerId()), "the database runs on the node");
        assertEquals("io.containerd.smolvm-pods.v2", node.containers().get(instance.getContainerId())
                .path("HostConfig").path("Runtime").asText());
        assertEquals(5432, instance.getContainerPort());
    }

    @Test
    @Order(2)
    void anEksClusterCreatedAfterTheDatabaseAdoptsTheNodeAndASecondIsRefused() {
        String nodeId = flociNode.record().orElseThrow().nodeId();

        eks.createCluster(request -> request.name(CLUSTER).roleArn(ROLE));

        await().atMost(30, TimeUnit.SECONDS).pollInterval(Duration.ofMillis(250))
                .until(() -> clusterStatus(CLUSTER).equals("ACTIVE"));
        assertEquals(1, driver.calls("create").size(), "the cluster adopts the node");
        assertEquals(nodeId, flociNode.record().orElseThrow().nodeId());
        assertEquals("https://localhost:" + flociNode.record().orElseThrow().apiPort(),
                eks.describeCluster(request -> request.name(CLUSTER)).cluster().endpoint());

        ResourceLimitExceededException refused = assertThrows(ResourceLimitExceededException.class,
                () -> eks.createCluster(request -> request.name("second").roleArn(ROLE)));
        assertThat(refused.awsErrorDetails().errorMessage(), containsString("one EKS cluster per smolvm-pods node"));
        assertThat(eks.listClusters().clusters(), not(hasItem("second")));
    }

    @Test
    @Order(3)
    void theDataApiReachesTheDatabaseThroughTheNodesTunnel() {
        DbInstance instance = rdsService.getDbInstance(DATABASE);
        FakeDockerNode node = driver.dockerNode();
        int before = node.requests().size();

        given().baseUri(endpoint.toString()).contentType("application/json")
                .body(Map.of("resourceArn", instance.getDbInstanceArn(),
                        "secretArn", instance.getMasterUserSecretArn(),
                        "database", "appdb", "sql", "select 1"))
                .when().post("/Execute")
                .then().statusCode(greaterThanOrEqualTo(400));

        List<FakeDockerNode.Request> tunnels = node.requests().subList(before, node.requests().size()).stream()
                .filter(request -> request.path().equals("/v1/tunnel")).toList();
        assertFalse(tunnels.isEmpty(), "the Data API went through the node's tunnel");
        assertEquals("/v1/tunnel?to=" + instance.getContainerHost() + "%3A5432", tunnels.getFirst().target());
    }

    @Test
    @Order(4)
    void deletingTheClusterKeepsTheNodeAndTheDatabase() {
        eks.deleteCluster(request -> request.name(CLUSTER));

        assertEquals(FlociNode.State.READY, flociNode.status().state());
        assertTrue(driver.calls("delete").isEmpty());
        assertEquals("available", instanceStatus(DATABASE));
        assertTrue(driver.dockerNode().containers().containsKey(rdsService.getDbInstance(DATABASE).getContainerId()));
    }

    @Test
    @Order(5)
    void deletingTheDatabaseKeepsTheNode() {
        rds.deleteDBInstance(request -> request.dbInstanceIdentifier(DATABASE).skipFinalSnapshot(true));

        assertThat(driver.dockerNode().containers().keySet(), empty());
        assertEquals(FlociNode.State.READY, flociNode.status().state());
        assertTrue(driver.calls("delete").isEmpty());
        assertTrue(driver.machine(flociNode.record().orElseThrow().nodeId()).orElseThrow().running());
    }

    @Test
    @Order(6)
    void aDbClusterWaitsForTheNodeToo() {
        given().baseUri(endpoint.toString()).when().post("/_floci/state/reset").then().statusCode(200);
        driver.reset();
        CountDownLatch create = driver.holdCreates();

        DBCluster created = rds.createDBCluster(request -> request.dbClusterIdentifier(DB_CLUSTER)
                .engine("aurora-postgresql").masterUsername("admin").masterUserPassword("password123")).dbCluster();

        assertEquals("creating", created.status());
        create.countDown();
        await().atMost(30, TimeUnit.SECONDS).pollInterval(Duration.ofMillis(250)).until(() ->
                rds.describeDBClusters(request -> request.dbClusterIdentifier(DB_CLUSTER)).dbClusters().getFirst()
                        .status().equals("available"));
        assertEquals(1, driver.dockerNode().containers().size());
        rds.deleteDBCluster(request -> request.dbClusterIdentifier(DB_CLUSTER).skipFinalSnapshot(true));
    }

    @Test
    @Order(7)
    void aStackWithADbClusterAndItsMemberCompletesWhileTheNodeIsCreated() {
        given().baseUri(endpoint.toString()).when().post("/_floci/state/reset").then().statusCode(200);
        driver.reset();
        CountDownLatch create = driver.holdCreates();
        String template = """
                {
                  "Resources": {
                    "Aurora": {
                      "Type": "AWS::RDS::DBCluster",
                      "Properties": {
                        "DBClusterIdentifier": "cfn-aurora",
                        "Engine": "aurora-postgresql",
                        "MasterUsername": "admin",
                        "MasterUserPassword": "password123"
                      }
                    },
                    "Writer": {
                      "Type": "AWS::RDS::DBInstance",
                      "Properties": {
                        "DBInstanceIdentifier": "cfn-aurora-writer",
                        "Engine": "aurora-postgresql",
                        "DBInstanceClass": "db.r6g.large",
                        "DBClusterIdentifier": {"Ref": "Aurora"}
                      }
                    }
                  }
                }
                """;

        given().baseUri(endpoint.toString()).contentType("application/x-www-form-urlencoded")
                .header("Authorization", CFN_AUTH)
                .formParam("Action", "CreateStack").formParam("StackName", "node-aurora-stack")
                .formParam("TemplateBody", template)
                .when().post("/").then().statusCode(200);

        CfnStackWaits.StackState state = CfnStackWaits.awaitTerminal("node-aurora-stack", CFN_AUTH);
        assertEquals("CREATE_COMPLETE", state.status(), state.reason());
        assertEquals("creating", instanceStatus("cfn-aurora-writer"));
        create.countDown();
        await().atMost(30, TimeUnit.SECONDS).pollInterval(Duration.ofMillis(250))
                .until(() -> instanceStatus("cfn-aurora-writer").equals("available"));
        assertEquals("available", rds.describeDBClusters(request -> request.dbClusterIdentifier("cfn-aurora"))
                .dbClusters().getFirst().status());
        assertEquals(1, driver.dockerNode().containers().size(), "the member shares its cluster's database");
    }
}
