package io.github.hectorvent.floci.services.rds;

import io.github.hectorvent.floci.testing.RdsMockProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static io.github.hectorvent.floci.testing.RdsRequests.rds;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * CreateDBCluster's EnableCloudwatchLogsExports and ModifyDBCluster's
 * CloudwatchLogsExportConfiguration set the log types DescribeDBClusters reports in
 * EnabledCloudwatchLogsExports.
 */
@QuarkusTest
@TestProfile(RdsMockProfile.class)
class RdsClusterLogExportsIntegrationTest {

    private static final String ID = "log-exports-cluster";
    private static final String RESULT =
            "DescribeDBClustersResponse.DescribeDBClustersResult.DBClusters.DBCluster.";

    @AfterEach
    void cleanUp() {
        rds("DeleteDBCluster").formParam("DBClusterIdentifier", ID)
                .formParam("SkipFinalSnapshot", "true").when().post("/");
    }

    @Test
    void createAndModifyDBCluster_setTheExportedLogTypes() {
        rds("CreateDBCluster")
                .formParam("DBClusterIdentifier", ID)
                .formParam("Engine", "aurora-postgresql")
                .formParam("MasterUsername", "adminuser")
                .formParam("MasterUserPassword", "secret123")
                .formParam("EnableCloudwatchLogsExports.member.1", "postgresql")
                .when().post("/").then().statusCode(200)
                .body("CreateDBClusterResponse.CreateDBClusterResult.DBCluster.EnabledCloudwatchLogsExports.member",
                        equalTo("postgresql"));

        rds("DescribeDBClusters").formParam("DBClusterIdentifier", ID)
                .when().post("/").then().statusCode(200)
                .body(RESULT + "EnabledCloudwatchLogsExports.member", equalTo("postgresql"));

        rds("ModifyDBCluster").formParam("DBClusterIdentifier", ID)
                .formParam("CloudwatchLogsExportConfiguration.DisableLogTypes.member.1", "postgresql")
                .when().post("/").then().statusCode(200);

        String body = rds("DescribeDBClusters").formParam("DBClusterIdentifier", ID)
                .when().post("/").then().statusCode(200).extract().asString();
        assertFalse(body.contains("EnabledCloudwatchLogsExports"));
    }
}
