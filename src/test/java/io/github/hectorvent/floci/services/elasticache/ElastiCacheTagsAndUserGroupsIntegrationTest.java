package io.github.hectorvent.floci.services.elasticache;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.response.ValidatableResponse;
import io.restassured.specification.RequestSpecification;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;

@QuarkusTest
class ElastiCacheTagsAndUserGroupsIntegrationTest {

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
                    .statusCode(200).body("CreateUserGroupResponse.CreateUserGroupResult.Status", equalTo("creating"))
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
