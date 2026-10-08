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
    void queryTagsRoundTrip() {
        String userArn = call("CreateUser", "UserId", "tags-it-default", "UserName", "default",
                "Engine", "redis", "NoPasswordRequired", "true", "AccessString", "on ~* +@all",
                "Tags.Tag.1.Key", "initial", "Tags.Tag.1.Value", "yes")
                .statusCode(200).extract().path("CreateUserResponse.CreateUserResult.ARN");
        try {
            call("AddTagsToResource", "ResourceName", userArn, "Tags.Tag.1.Key", "env", "Tags.Tag.1.Value", "test")
                    .statusCode(200).body("AddTagsToResourceResponse.AddTagsToResourceResult.TagList.Tag.Key",
                            hasItems("initial", "env"));
            call("RemoveTagsFromResource", "ResourceName", userArn, "TagKeys.member.1", "initial").statusCode(200);
            call("ListTagsForResource", "ResourceName", userArn).statusCode(200)
                    .body("ListTagsForResourceResponse.ListTagsForResourceResult.TagList.Tag.Value", equalTo("test"));
        } finally {
            call("DeleteUser", "UserId", "tags-it-default");
        }
    }
}
