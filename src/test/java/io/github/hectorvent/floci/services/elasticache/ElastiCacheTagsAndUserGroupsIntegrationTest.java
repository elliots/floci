package io.github.hectorvent.floci.services.elasticache;

import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.response.ValidatableResponse;
import io.restassured.specification.RequestSpecification;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.elasticache.ElastiCacheClient;
import software.amazon.awssdk.services.elasticache.model.DescribeUserGroupsResponse;
import software.amazon.awssdk.services.elasticache.model.Tag;

import java.net.URI;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class ElastiCacheTagsAndUserGroupsIntegrationTest {
    @TestHTTPResource
    URI endpoint;

    @Test
    void sdkReadsPagedGroupsAndPersistedResourceTags() {
        try (ElastiCacheClient client = ElastiCacheClient.builder().endpointOverride(endpoint)
                .region(Region.US_EAST_1).credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("test", "test"))).build()) {
            String first = "sdk-page-group-a";
            String second = "sdk-page-group-b";
            String arn = client.createUserGroup(request -> request.userGroupId(first).engine("valkey")
                    .tags(Tag.builder().key("environment").value("test").build())).arn();
            client.createUserGroup(request -> request.userGroupId(second).engine("valkey"));
            try {
                DescribeUserGroupsResponse page = client.describeUserGroups(request -> request.maxRecords(1));
                assertEquals(1, page.userGroups().size());
                assertTrue(page.marker() != null && !page.marker().isEmpty());
                DescribeUserGroupsResponse next = client.describeUserGroups(request -> request.maxRecords(1)
                        .marker(page.marker()));
                assertEquals(1, next.userGroups().size());
                assertTrue(!page.userGroups().getFirst().userGroupId().equals(next.userGroups().getFirst().userGroupId()));
                assertEquals("test", client.listTagsForResource(request -> request.resourceName(arn))
                        .tagList().getFirst().value());
                client.addTagsToResource(request -> request.resourceName(arn)
                        .tags(Tag.builder().key("environment").value("updated").build()));
                assertEquals("updated", client.listTagsForResource(request -> request.resourceName(arn))
                        .tagList().getFirst().value());
            } finally {
                client.deleteUserGroup(request -> request.userGroupId(first));
                client.deleteUserGroup(request -> request.userGroupId(second));
            }
        }
    }


    private ValidatableResponse call(String action, String... params) {
        RequestSpecification request = given().header("Authorization",
                "AWS4-HMAC-SHA256 Credential=test/20261008/us-east-1/elasticache/aws4_request")
                .formParam("Action", action).formParam("Version", "2015-02-02");
        for (int i = 0; i < params.length; i += 2) {
            request.formParam(params[i], params[i + 1]);
        }
        return request.post("/").then();
    }

    @Test
    void describeUserGroupsPaginatesAndRejectsInvalidMarkers() {
        call("CreateUserGroup", "UserGroupId", "pagination-it-a", "Engine", "valkey").statusCode(200);
        call("CreateUserGroup", "UserGroupId", "pagination-it-b", "Engine", "valkey").statusCode(200);
        try {
            String marker = call("DescribeUserGroups", "MaxRecords", "1").statusCode(200)
                    .body("DescribeUserGroupsResponse.DescribeUserGroupsResult.UserGroups.member.size()", equalTo(1))
                    .extract().path("DescribeUserGroupsResponse.DescribeUserGroupsResult.Marker");
            call("DescribeUserGroups", "MaxRecords", "1", "Marker", marker).statusCode(200)
                    .body("DescribeUserGroupsResponse.DescribeUserGroupsResult.UserGroups.member.size()", equalTo(1));
            call("DescribeUserGroups", "Marker", "not-a-returned-marker").statusCode(400)
                    .body("ErrorResponse.Error.Code", equalTo("InvalidParameterValue"));
        } finally {
            call("DeleteUserGroup", "UserGroupId", "pagination-it-a");
            call("DeleteUserGroup", "UserGroupId", "pagination-it-b");
        }
    }

    @Test
    void queryTagsAndUserGroupMembershipRoundTrip() {
        String userArn = call("CreateUser", "UserId", "tags-it-default", "UserName", "default",
                "Engine", "redis", "NoPasswordRequired", "true", "AccessString", "on ~* +@all",
                "Tags.Tag.1.Key", "initial", "Tags.Tag.1.Value", "yes")
                .statusCode(200).extract().path("CreateUserResponse.CreateUserResult.ARN");
        call("CreateUser", "UserId", "tags-it-extra", "UserName", "extra", "Engine", "redis",
                "NoPasswordRequired", "true", "AccessString", "on ~* +@all").statusCode(200);
        try {
            call("AddTagsToResource", "ResourceName", userArn, "Tags.Tag.1.Key", "env", "Tags.Tag.1.Value", "test")
                    .statusCode(200).body("AddTagsToResourceResponse.AddTagsToResourceResult.TagList.Tag.Key",
                            hasItems("initial", "env"));
            call("RemoveTagsFromResource", "ResourceName", userArn, "TagKeys.member.1", "initial").statusCode(200);
            call("ListTagsForResource", "ResourceName", userArn).statusCode(200)
                    .body("ListTagsForResourceResponse.ListTagsForResourceResult.TagList.Tag.Value", equalTo("test"));
            String groupArn = call("CreateUserGroup", "UserGroupId", "tags-it-team", "Engine", "redis",
                    "UserIds.member.1", "tags-it-default", "Tags.Tag.1.Key", "team", "Tags.Tag.1.Value", "test")
                    .statusCode(200).body("CreateUserGroupResponse.CreateUserGroupResult.Status", equalTo("active"))
                    .extract().path("CreateUserGroupResponse.CreateUserGroupResult.ARN");
            call("DescribeUserGroups", "UserGroupId", "tags-it-team").statusCode(200)
                    .body("DescribeUserGroupsResponse.DescribeUserGroupsResult.UserGroups.member.Status", equalTo("active"));
            call("ModifyUserGroup", "UserGroupId", "tags-it-team", "UserIdsToAdd.member.1", "tags-it-extra")
                    .statusCode(200).body("ModifyUserGroupResponse.ModifyUserGroupResult.UserIds.member",
                            hasItems("tags-it-default", "tags-it-extra"));
            call("DescribeUsers", "UserId", "tags-it-extra").statusCode(200)
                    .body("DescribeUsersResponse.DescribeUsersResult.Users.member.UserGroupIds.member", equalTo("tags-it-team"));
            call("ListTagsForResource", "ResourceName", groupArn).statusCode(200)
                    .body("ListTagsForResourceResponse.ListTagsForResourceResult.TagList.Tag.Key", equalTo("team"));
            call("ModifyUserGroup", "UserGroupId", "tags-it-team", "UserIdsToRemove.member.1", "tags-it-extra").statusCode(200);
            call("DeleteUserGroup", "UserGroupId", "tags-it-team").statusCode(200)
                    .body("DeleteUserGroupResponse.DeleteUserGroupResult.Status", equalTo("deleting"));
            call("DescribeUserGroups", "UserGroupId", "tags-it-team").statusCode(404)
                    .body("ErrorResponse.Error.Code", equalTo("UserGroupNotFound"));
        } finally {
            call("DeleteUserGroup", "UserGroupId", "tags-it-team");
            call("DeleteUser", "UserId", "tags-it-extra");
            call("DeleteUser", "UserId", "tags-it-default");
        }
    }
}
