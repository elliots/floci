package io.github.hectorvent.floci.services.rds;

import io.github.hectorvent.floci.services.rds.container.RdsRuntimeTestSupport;
import io.github.hectorvent.floci.services.rds.model.DbCluster;
import io.github.hectorvent.floci.services.rds.model.DbInstance;
import io.github.hectorvent.floci.testing.RdsRuntimeTestProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.specification.RequestSpecification;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.Socket;

import static io.restassured.RestAssured.given;
import static io.restassured.http.ContentType.URLENC;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Stop and start against real PostgreSQL backends: a stopped instance has no active backend and
 * its endpoint refuses connections, a started one has the same endpoint and the row written
 * before the stop, and a cluster carries its member along. Statuses follow the user guide:
 * "stopping" in the stop response, "stopped" afterwards, "starting" then "available".
 */
@QuarkusTest
@Tag("runtime")
@TestProfile(RdsRuntimeTestProfile.class)
class RdsStopStartIntegrationTest extends RdsRuntimeTestSupport {

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
    void stoppedInstanceKeepsItsDataAndEndpointForTheStart() throws Exception {
        String id = "stop-db-" + Long.toString(System.nanoTime(), 36);
        query("CreateDBInstance")
                .formParam("DBInstanceIdentifier", id)
                .formParam("Engine", "postgres")
                .formParam("MasterUsername", "admin")
                .formParam("MasterUserPassword", "password123")
                .formParam("DBName", "appdb")
                .formParam("DBInstanceClass", "db.t3.micro")
                .formParam("AllocatedStorage", "5")
        .when().post("/").then().statusCode(200);
        waitForInstance(id, "available");
        DbInstance running = rdsService.getDbInstance(id);
        String previousBackendId = running.getContainerId();
        int port = running.getEndpoint().port();
        postgresSql(port, "CREATE TABLE marker(v text); INSERT INTO marker VALUES ('before-stop');");
        assertTrue(portOpen(port), "the endpoint must accept connections while available");

        query("StopDBInstance")
                .formParam("DBInstanceIdentifier", id)
        .when().post("/").then().statusCode(200)
                .body(containsString("<StopDBInstanceResult>"))
                .body(containsString("<DBInstanceStatus>stopping</DBInstanceStatus>"));
        waitForInstance(id, "stopped");
        DbInstance stopped = rdsService.getDbInstance(id);
        assertNull(stopped.getContainerId(), "a stopped instance has no active backend");
        assertBackendStopped(running.getDbInstanceArn(), previousBackendId);
        assertFalse(portOpen(port), "the endpoint refuses connections while stopped");

        query("ModifyDBInstance")
                .formParam("DBInstanceIdentifier", id)
                .formParam("MasterUserPassword", "changed")
        .when().post("/").then().statusCode(400)
                .body(containsString("<Code>InvalidDBInstanceState</Code>"));
        query("StopDBInstance")
                .formParam("DBInstanceIdentifier", id)
        .when().post("/").then().statusCode(400)
                .body(containsString("<Code>InvalidDBInstanceState</Code>"));

        query("StartDBInstance")
                .formParam("DBInstanceIdentifier", id)
        .when().post("/").then().statusCode(200)
                .body(containsString("<StartDBInstanceResult>"))
                .body(containsString("<DBInstanceStatus>starting</DBInstanceStatus>"));
        waitForInstance(id, "available");
        DbInstance started = rdsService.getDbInstance(id);
        assertTrue(started.getEndpoint().port() == port, "the endpoint keeps its port");
        assertTrue(portOpen(port), "the endpoint accepts connections again");
        assertTrue(postgresSql(started.getEndpoint().port(), "SELECT v FROM marker;").contains("before-stop"),
                "the row written before the stop must survive on the volume");

        query("DeleteDBInstance").formParam("DBInstanceIdentifier", id).formParam("SkipFinalSnapshot", "true")
                .when().post("/").then().statusCode(200);
    }

    @Test
    void stoppedClusterCarriesItsMemberAndRebootKeepsTheData() throws Exception {
        String cluster = "stop-cluster-" + Long.toString(System.nanoTime(), 36);
        String member = cluster + "-1";
        query("CreateDBCluster")
                .formParam("DBClusterIdentifier", cluster)
                .formParam("Engine", "aurora-postgresql")
                .formParam("EngineVersion", "16.3")
                .formParam("MasterUsername", "admin")
                .formParam("MasterUserPassword", "password123")
                .formParam("DatabaseName", "appdb")
        .when().post("/").then().statusCode(200);
        waitForCluster(cluster, "available");
        query("CreateDBInstance")
                .formParam("DBInstanceIdentifier", member)
                .formParam("DBClusterIdentifier", cluster)
                .formParam("Engine", "aurora-postgresql")
                .formParam("DBInstanceClass", "db.r6g.large")
        .when().post("/").then().statusCode(200);
        waitForInstance(member, "available");
        DbCluster running = rdsService.getDbCluster(cluster);
        String previousBackendId = running.getContainerId();
        postgresSql(running.getEndpoint().port(), "CREATE TABLE marker(v text); INSERT INTO marker VALUES ('cluster-row');");

        query("StopDBCluster")
                .formParam("DBClusterIdentifier", cluster)
        .when().post("/").then().statusCode(200)
                .body(containsString("<StopDBClusterResult>"))
                .body(containsString("<Status>stopping</Status>"));
        waitForCluster(cluster, "stopped");
        waitForInstance(member, "stopped");
        assertNull(rdsService.getDbCluster(cluster).getContainerId());
        assertBackendStopped(running.getDbClusterArn(), previousBackendId);

        query("StartDBCluster")
                .formParam("DBClusterIdentifier", cluster)
        .when().post("/").then().statusCode(200)
                .body(containsString("<Status>starting</Status>"));
        waitForCluster(cluster, "available");
        waitForInstance(member, "available");
        assertTrue(postgresSql(rdsService.getDbCluster(cluster).getEndpoint().port(), "SELECT v FROM marker;").contains("cluster-row"));

        query("RebootDBCluster")
                .formParam("DBClusterIdentifier", cluster)
        .when().post("/").then().statusCode(200)
                .body(containsString("<RebootDBClusterResult>"))
                .body(containsString("<Status>rebooting</Status>"));
        waitForCluster(cluster, "available");
        assertTrue(postgresSql(rdsService.getDbCluster(cluster).getEndpoint().port(), "SELECT v FROM marker;").contains("cluster-row"));

        query("DeleteDBInstance").formParam("DBInstanceIdentifier", member).formParam("SkipFinalSnapshot", "true")
                .when().post("/").then().statusCode(200);
        query("DeleteDBCluster").formParam("DBClusterIdentifier", cluster).formParam("SkipFinalSnapshot", "true")
                .when().post("/").then().statusCode(200);
    }

    private static boolean portOpen(int port) {
        try (Socket socket = new Socket("localhost", port)) {
            return socket.isConnected();
        } catch (IOException e) {
            return false;
        }
    }

    private void waitForInstance(String id, String expected) throws Exception {
        for (int i = 0; i < 60; i++) {
            String status = query("DescribeDBInstances").formParam("DBInstanceIdentifier", id)
                    .when().post("/").then().extract().xmlPath().getString(
                            "DescribeDBInstancesResponse.DescribeDBInstancesResult.DBInstances.DBInstance.DBInstanceStatus");
            if (expected.equals(status)) {
                return;
            }
            Thread.sleep(1000);
        }
        throw new RuntimeException("DB instance " + id + " did not reach " + expected + ".");
    }

    private void waitForCluster(String id, String expected) throws Exception {
        for (int i = 0; i < 60; i++) {
            String status = query("DescribeDBClusters").formParam("DBClusterIdentifier", id)
                    .when().post("/").then().extract().xmlPath().getString(
                            "DescribeDBClustersResponse.DescribeDBClustersResult.DBClusters.DBCluster.Status");
            if (expected.equals(status)) {
                return;
            }
            Thread.sleep(1000);
        }
        throw new RuntimeException("DB cluster " + id + " did not reach " + expected + ".");
    }

}
