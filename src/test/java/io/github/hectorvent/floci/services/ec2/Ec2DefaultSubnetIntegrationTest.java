package io.github.hectorvent.floci.services.ec2;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.response.ValidatableResponse;
import io.restassured.specification.RequestSpecification;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

@QuarkusTest
class Ec2DefaultSubnetIntegrationTest {

    private ValidatableResponse call(String action, String... params) {
        RequestSpecification request = given().header("Authorization",
                "AWS4-HMAC-SHA256 Credential=default-subnet-it/20261008/eu-west-2/ec2/aws4_request")
                .formParam("Action", action).formParam("Version", "2016-11-15");
        for (int i = 0; i < params.length; i += 2) {
            request.formParam(params[i], params[i + 1]);
        }
        return request.post("/").then();
    }

    @Test
    void defaultSubnetQueryShapeErrorsAndDryRun() {
        String id = call("DescribeSubnets", "Filter.1.Name", "availability-zone", "Filter.1.Value.1", "eu-west-2a")
                .statusCode(200).extract().path("DescribeSubnetsResponse.subnetSet.item.subnetId");
        call("DeleteSubnet", "SubnetId", id).statusCode(200);
        call("CreateDefaultSubnet", "AvailabilityZone", "eu-west-2a", "DryRun", "true").statusCode(412)
                .body("Response.Errors.Error.Code", equalTo("DryRunOperation"));
        call("CreateDefaultSubnet", "AvailabilityZone", "eu-west-2a").statusCode(200)
                .body("CreateDefaultSubnetResponse.subnet.defaultForAz", equalTo("true"))
                .body("CreateDefaultSubnetResponse.subnet.mapPublicIpOnLaunch", equalTo("true"))
                .body("CreateDefaultSubnetResponse.subnet.availableIpAddressCount", equalTo("4091"))
                .body("CreateDefaultSubnetResponse.subnet.cidrBlock", equalTo("172.31.0.0/20"));
        call("CreateDefaultSubnet", "AvailabilityZone", "eu-west-2a").statusCode(400)
                .body("Response.Errors.Error.Code", equalTo("DefaultSubnetAlreadyExistsInAvailabilityZone"));
    }
}
