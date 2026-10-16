package io.github.hectorvent.floci.services.rds;

import io.github.hectorvent.floci.services.rds.container.RdsRuntimeTestSupport;
import io.github.hectorvent.floci.services.rds.model.DbCluster;
import io.github.hectorvent.floci.testing.RdsRuntimeTestProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.specification.RequestSpecification;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static io.restassured.http.ContentType.URLENC;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The cluster snapshot lifecycle against real PostgreSQL backends: a row written before the
 * snapshot comes back from a cluster restored from a copy of that snapshot, the source snapshot
 * can be deleted without touching the copy, and each refusal carries the AWS error.
 */
@QuarkusTest
@Tag("runtime")
@TestProfile(RdsRuntimeTestProfile.class)
class RdsClusterSnapshotLifecycleIntegrationTest extends RdsRuntimeTestSupport {

    private static final String AUTH =
            "AWS4-HMAC-SHA256 Credential=test/20260921/us-east-1/rds/aws4_request, "
            + "SignedHeaders=content-type;host, Signature=test";

    @Inject
    RdsService rdsService;

    private static RequestSpecification query(String action) {
        return given().header("Authorization", AUTH)
                .contentType(URLENC)
                .formParam("Action", action)
                .formParam("Version", "2014-10-31");
    }

    @Test
    void clusterSnapshotRoundTripsTheClustersDataIntoARestoredCluster() throws Exception {
        String suffix = Long.toString(System.nanoTime(), 36);
        String cluster = "csnap-src-" + suffix;
        String snapshot = "csnap-" + suffix;
        String copy = "csnap-copy-" + suffix;
        String restored = "csnap-restored-" + suffix;

        query("CreateDBCluster")
                .formParam("DBClusterIdentifier", cluster)
                .formParam("Engine", "aurora-postgresql")
                .formParam("EngineVersion", "16.3")
                .formParam("MasterUsername", "admin")
                .formParam("MasterUserPassword", "password123")
                .formParam("DatabaseName", "appdb")
        .when().post("/").then().statusCode(200);
        waitForCluster(cluster);
        DbCluster source = rdsService.getDbCluster(cluster);
        postgresSql(source.getEndpoint().port(), "CREATE TABLE marker(v text); INSERT INTO marker VALUES ('cluster-row');");

        query("CreateDBClusterSnapshot")
                .formParam("DBClusterSnapshotIdentifier", snapshot)
                .formParam("DBClusterIdentifier", cluster)
                .formParam("Tags.Tag.1.Key", "owner")
                .formParam("Tags.Tag.1.Value", "platform")
        .when().post("/").then().statusCode(200)
                .body(containsString("<CreateDBClusterSnapshotResult>"))
                .body(containsString("<Engine>aurora-postgresql</Engine>"))
                .body(containsString("<Status>available</Status>"))
                .body(containsString("<SnapshotType>manual</SnapshotType>"))
                .body(containsString("<PercentProgress>100</PercentProgress>"))
                .body(containsString(":cluster-snapshot:" + snapshot + "</DBClusterSnapshotArn>"));

        query("DescribeDBClusterSnapshots")
                .formParam("DBClusterIdentifier", cluster)
        .when().post("/").then().statusCode(200)
                .body(containsString("<DBClusterSnapshotIdentifier>" + snapshot + "</DBClusterSnapshotIdentifier>"));

        String sourceArn = "arn:aws:rds:us-east-1:000000000000:cluster-snapshot:" + snapshot;
        query("CopyDBClusterSnapshot")
                .formParam("SourceDBClusterSnapshotIdentifier", sourceArn)
                .formParam("TargetDBClusterSnapshotIdentifier", copy)
                .formParam("CopyTags", "true")
        .when().post("/").then().statusCode(200)
                .body(containsString("<SourceDBClusterSnapshotArn>" + sourceArn + "</SourceDBClusterSnapshotArn>"))
                .body(containsString("<Value>platform</Value>"));

        query("ModifyDBClusterSnapshotAttribute")
                .formParam("DBClusterSnapshotIdentifier", copy)
                .formParam("AttributeName", "restore")
                .formParam("ValuesToAdd.member.1", "all")
        .when().post("/").then().statusCode(200)
                .body(containsString("<AttributeValue>all</AttributeValue>"));

        query("DeleteDBClusterSnapshot")
                .formParam("DBClusterSnapshotIdentifier", snapshot)
        .when().post("/").then().statusCode(200)
                .body(containsString("<Status>deleted</Status>"));
        query("DescribeDBClusterSnapshots")
                .formParam("DBClusterSnapshotIdentifier", snapshot)
        .when().post("/").then().statusCode(404)
                .body(containsString("<Code>DBClusterSnapshotNotFoundFault</Code>"));

        // Restore by ARN, the form aws_rds_cluster's snapshot_identifier is commonly given.
        query("RestoreDBClusterFromSnapshot")
                .formParam("DBClusterIdentifier", restored)
                .formParam("SnapshotIdentifier", "arn:aws:rds:us-east-1:000000000000:cluster-snapshot:" + copy)
                .formParam("Engine", "aurora-postgresql")
        .when().post("/").then().statusCode(200)
                .body(containsString("<RestoreDBClusterFromSnapshotResult>"))
                .body(containsString("<DBClusterIdentifier>" + restored + "</DBClusterIdentifier>"));
        waitForCluster(restored);
        DbCluster restoredCluster = rdsService.getDbCluster(restored);
        assertTrue(postgresSql(restoredCluster.getEndpoint().port(), "SELECT v FROM marker;").contains("cluster-row"),
                "the row written before the cluster snapshot must come back from the copy");

        query("DeleteDBClusterSnapshot").formParam("DBClusterSnapshotIdentifier", copy)
                .when().post("/").then().statusCode(200);
        query("DeleteDBCluster").formParam("DBClusterIdentifier", restored).formParam("SkipFinalSnapshot", "true")
                .when().post("/").then().statusCode(200);
        query("DeleteDBCluster").formParam("DBClusterIdentifier", cluster).formParam("SkipFinalSnapshot", "true")
                .when().post("/").then().statusCode(200);
    }

    private void waitForCluster(String clusterId) throws Exception {
        for (int i = 0; i < 60; i++) {
            String status = query("DescribeDBClusters")
                    .formParam("DBClusterIdentifier", clusterId)
            .when().post("/").then()
                    .extract().xmlPath().getString(
                            "DescribeDBClustersResponse.DescribeDBClustersResult.DBClusters.DBCluster.Status");
            if ("available".equals(status)) {
                return;
            }
            Thread.sleep(1000);
        }
        throw new RuntimeException("DB cluster " + clusterId + " did not become available.");
    }

}
