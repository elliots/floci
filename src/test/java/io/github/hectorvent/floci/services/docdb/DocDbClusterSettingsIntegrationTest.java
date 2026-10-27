package io.github.hectorvent.floci.services.docdb;

import io.github.hectorvent.floci.services.ec2.Ec2Service;
import io.github.hectorvent.floci.services.ec2.model.SecurityGroup;
import io.github.hectorvent.floci.services.kms.KmsService;
import io.github.hectorvent.floci.testing.RdsAndDocDbMockProfile;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBClusterMember;

import java.net.URI;
import java.util.List;

import static io.github.hectorvent.floci.testing.RdsRequests.rds;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The settings a DocumentDB cluster is created with come back from DescribeDBClusters, as on a
 * live account: through the Query protocol, with a real KMS key from the KMS store and the
 * default subnet group, parameter group and security group.
 */
@QuarkusTest
@TestProfile(RdsAndDocDbMockProfile.class)
class DocDbClusterSettingsIntegrationTest {

    private static final String ID = "settings-cluster";

    @Inject
    KmsService kmsService;

    @Inject
    Ec2Service ec2Service;

    @TestHTTPResource
    URI endpoint;

    @AfterEach
    void cleanUp() {
        rds("DeleteDBInstance").formParam("DBInstanceIdentifier", ID + "-1").when().post("/");
        rds("DeleteDBCluster").formParam("DBClusterIdentifier", ID).formParam("SkipFinalSnapshot", "true").when().post("/");
    }

    @Test
    void membersAreDbClusterMemberElements() {
        rds("CreateDBCluster")
                .formParam("DBClusterIdentifier", ID)
                .formParam("Engine", "docdb")
                .formParam("MasterUsername", "u")
                .formParam("MasterUserPassword", "secret99password")
                .when().post("/").then().statusCode(200);
        rds("CreateDBInstance")
                .formParam("DBInstanceIdentifier", ID + "-1")
                .formParam("DBInstanceClass", "db.t3.medium")
                .formParam("Engine", "docdb")
                .formParam("DBClusterIdentifier", ID)
                .when().post("/").then().statusCode(200);

        rds("DescribeDBClusters").formParam("DBClusterIdentifier", ID)
                .when().post("/").then().statusCode(200)
                .body(containsString("<DBClusterMembers><DBClusterMember><DBInstanceIdentifier>" + ID
                        + "-1</DBInstanceIdentifier><IsClusterWriter>true</IsClusterWriter></DBClusterMember>"
                        + "</DBClusterMembers>"));
        try (RdsClient client = RdsClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .build()) {
            List<DBClusterMember> members = client.describeDBClusters(request -> request.dbClusterIdentifier(ID))
                    .dbClusters().getFirst().dbClusterMembers();
            assertEquals(List.of(ID + "-1"), members.stream().map(DBClusterMember::dbInstanceIdentifier).toList());
        }
    }

    /**
     * A security group that exists is the case a live stack always sends, and the one a probe
     * with a made-up id never reaches, because an unknown id is refused before its record is
     * consulted. 2.0.1 answered it with an internal error.
     */
    @Test
    void anExistingSecurityGroupIsAcceptedOnCreateAndModify() {
        String vpcId = ec2Service.createVpc("us-east-1", "10.9.0.0/16", false).getVpcId();
        SecurityGroup first = ec2Service.createSecurityGroup("us-east-1", "docdb-first", "d", vpcId);
        SecurityGroup second = ec2Service.createSecurityGroup("us-east-1", "docdb-second", "d", vpcId);

        rds("CreateDBCluster")
                .formParam("DBClusterIdentifier", ID)
                .formParam("Engine", "docdb")
                .formParam("MasterUsername", "u")
                .formParam("MasterUserPassword", "secret99password")
                .formParam("VpcSecurityGroupIds.VpcSecurityGroupId.1", first.getGroupId())
                .when().post("/").then().statusCode(200)
                .body(containsString("<VpcSecurityGroupId>" + first.getGroupId() + "</VpcSecurityGroupId>"));

        rds("ModifyDBCluster")
                .formParam("DBClusterIdentifier", ID)
                .formParam("VpcSecurityGroupIds.VpcSecurityGroupId.1", first.getGroupId())
                .formParam("VpcSecurityGroupIds.VpcSecurityGroupId.2", second.getGroupId())
                .when().post("/").then().statusCode(200);

        rds("DescribeDBClusters").formParam("DBClusterIdentifier", ID)
                .when().post("/").then().statusCode(200)
                .body(containsString("<VpcSecurityGroupId>" + first.getGroupId() + "</VpcSecurityGroupId>"))
                .body(containsString("<VpcSecurityGroupId>" + second.getGroupId() + "</VpcSecurityGroupId>"));
    }

    @Test
    void settingsGivenOnCreateComeBackFromDescribeAndModifyChangesThem() {
        String keyArn = kmsService.createKey("docdb settings", "us-east-1").getArn();

        rds("CreateDBCluster")
                .formParam("DBClusterIdentifier", ID)
                .formParam("Engine", "docdb")
                .formParam("MasterUsername", "u")
                .formParam("MasterUserPassword", "secret99password")
                .formParam("StorageEncrypted", "true")
                .formParam("KmsKeyId", keyArn)
                .formParam("BackupRetentionPeriod", "5")
                .formParam("PreferredBackupWindow", "23:30-00:00")
                .formParam("PreferredMaintenanceWindow", "Sun:03:00-Sun:04:00")
                .formParam("Tags.Tag.1.Key", "Name")
                .formParam("Tags.Tag.1.Value", ID)
        .when().post("/")
        .then().statusCode(200)
            .body(containsString("<KmsKeyId>" + keyArn + "</KmsKeyId>"));

        rds("DescribeDBClusters").formParam("DBClusterIdentifier", ID)
        .when().post("/")
        .then().statusCode(200)
            .body(containsString("<DBSubnetGroup>default</DBSubnetGroup>"))
            .body(containsString("<DBClusterParameterGroup>default.docdb5.0</DBClusterParameterGroup>"))
            .body(containsString("<StorageEncrypted>true</StorageEncrypted>"))
            .body(containsString("<KmsKeyId>" + keyArn + "</KmsKeyId>"))
            .body(containsString("<BackupRetentionPeriod>5</BackupRetentionPeriod>"))
            .body(containsString("<PreferredBackupWindow>23:30-00:00</PreferredBackupWindow>"))
            .body(containsString("<PreferredMaintenanceWindow>sun:03:00-sun:04:00</PreferredMaintenanceWindow>"))
            .body(containsString("<VpcSecurityGroupId>sg-"));

        rds("ListTagsForResource").formParam("ResourceName", "arn:aws:rds:us-east-1:000000000000:cluster:" + ID)
        .when().post("/").then().statusCode(200).body(containsString("<Value>" + ID + "</Value>"));

        rds("CreateDBInstance")
                .formParam("DBInstanceIdentifier", ID + "-1")
                .formParam("DBInstanceClass", "db.t3.medium")
                .formParam("Engine", "docdb")
                .formParam("DBClusterIdentifier", ID)
                .formParam("AutoMinorVersionUpgrade", "false")
                .formParam("Tags.Tag.1.Key", "Name")
                .formParam("Tags.Tag.1.Value", ID + "-1")
        .when().post("/")
        .then().statusCode(200)
            .body(containsString("<AutoMinorVersionUpgrade>false</AutoMinorVersionUpgrade>"))
            .body(containsString("<PreferredMaintenanceWindow>sun:03:00-sun:04:00</PreferredMaintenanceWindow>"))
            .body(containsString("<StorageEncrypted>true</StorageEncrypted>"));

        rds("ModifyDBCluster").formParam("DBClusterIdentifier", ID)
                .formParam("BackupRetentionPeriod", "7")
                .formParam("PreferredBackupWindow", "02:00-02:30")
        .when().post("/")
        .then().statusCode(200)
            .body(containsString("<BackupRetentionPeriod>7</BackupRetentionPeriod>"))
            .body(containsString("<PreferredBackupWindow>02:00-02:30</PreferredBackupWindow>"));

        rds("ModifyDBCluster").formParam("DBClusterIdentifier", ID)
                .formParam("PreferredMaintenanceWindow", "mon:02:15-mon:02:45")
        .when().post("/")
        .then().statusCode(400)
            .body(containsString("The backup window and maintenance window must not overlap."));
    }
}
