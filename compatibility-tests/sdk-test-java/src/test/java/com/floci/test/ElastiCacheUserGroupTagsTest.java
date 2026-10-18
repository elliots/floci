package com.floci.test;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.elasticache.ElastiCacheClient;
import software.amazon.awssdk.services.elasticache.model.CreateUserGroupResponse;
import software.amazon.awssdk.services.elasticache.model.CreateUserResponse;
import software.amazon.awssdk.services.elasticache.model.Tag;
import software.amazon.awssdk.services.elasticache.model.UserGroupNotFoundException;
import software.amazon.awssdk.services.elasticache.model.UserNotFoundException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("ElastiCache user groups and tags")
class ElastiCacheUserGroupTagsTest {

    @Test
    @DisplayName("SDK deserializes tags, user-group lifecycle, membership, and typed errors")
    void tagsAndUserGroups() {
        String owner = TestFixtures.uniqueName("tag-owner");
        String extra = TestFixtures.uniqueName("tag-extra");
        String group = TestFixtures.uniqueName("tag-group");
        try (ElastiCacheClient client = TestFixtures.elastiCacheClient()) {
            CreateUserResponse user = client.createUser(r -> r.userId(owner).userName("default").engine("redis")
                    .accessString("on ~* +@all").noPasswordRequired(true)
                    .tags(Tag.builder().key("initial").value("yes").build()));
            client.createUser(r -> r.userId(extra).userName(extra).engine("redis")
                    .accessString("on ~* +@all").noPasswordRequired(true));
            try {
                assertThat(client.addTagsToResource(r -> r.resourceName(user.arn())
                        .tags(Tag.builder().key("env").value("test").build())).tagList())
                        .extracting(Tag::key).containsExactlyInAnyOrder("initial", "env");
                client.removeTagsFromResource(r -> r.resourceName(user.arn()).tagKeys("initial"));
                assertThat(client.listTagsForResource(r -> r.resourceName(user.arn())).tagList())
                        .containsExactly(Tag.builder().key("env").value("test").build());
                CreateUserGroupResponse created = client.createUserGroup(r -> r.userGroupId(group).engine("redis")
                        .userIds(owner).tags(Tag.builder().key("team").value("test").build()));
                assertThat(created.arn()).endsWith(":usergroup:" + group);
                assertThat(client.describeUserGroups(r -> r.userGroupId(group)).userGroups().get(0).status())
                        .isEqualTo("active");
                assertThat(client.modifyUserGroup(r -> r.userGroupId(group).userIdsToAdd(extra)).userIds())
                        .containsExactlyInAnyOrder(owner, extra);
                assertThat(client.describeUsers(r -> r.userId(extra)).users().get(0).userGroupIds()).contains(group);
                assertThat(client.modifyUserGroup(r -> r.userGroupId(group).userIdsToRemove(extra)).userIds())
                        .containsExactly(owner);
                client.addTagsToResource(r -> r.resourceName(created.arn()).tags(Tag.builder().key("new").value("yes").build()));
                assertThat(client.removeTagsFromResource(r -> r.resourceName(created.arn()).tagKeys("new")).tagList())
                        .containsExactly(Tag.builder().key("team").value("test").build());
                assertThat(client.deleteUserGroup(r -> r.userGroupId(group)).status()).isEqualTo("deleting");
                assertThatThrownBy(() -> client.describeUserGroups(r -> r.userGroupId(group)))
                        .isInstanceOf(UserGroupNotFoundException.class);
                String missingArn = user.arn().replace(":user:" + owner, ":user:missing-tag-sdk-user");
                assertThatThrownBy(() -> client.listTagsForResource(r -> r.resourceName(missingArn)))
                        .isInstanceOf(UserNotFoundException.class);
            } finally {
                try {
                    client.deleteUserGroup(r -> r.userGroupId(group));
                } catch (UserGroupNotFoundException ignored) {
                    // The successful test already deleted the group.
                }
                client.deleteUser(r -> r.userId(extra));
                client.deleteUser(r -> r.userId(owner));
            }
        }
    }
}
