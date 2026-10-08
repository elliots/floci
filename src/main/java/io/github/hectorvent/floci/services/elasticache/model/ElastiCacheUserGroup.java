package io.github.hectorvent.floci.services.elasticache.model;

import io.quarkus.runtime.annotations.RegisterForReflection;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RegisterForReflection
public class ElastiCacheUserGroup {

    private String userGroupId;
    private String engine;
    private String status;
    private String arn;
    private List<String> userIds = new ArrayList<>();
    private Map<String, String> tags = new LinkedHashMap<>();

    public String getUserGroupId() { return userGroupId; }
    public void setUserGroupId(String userGroupId) { this.userGroupId = userGroupId; }
    public String getEngine() { return engine; }
    public void setEngine(String engine) { this.engine = engine; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public String getArn() { return arn; }
    public void setArn(String arn) { this.arn = arn; }
    public List<String> getUserIds() { return userIds; }
    public void setUserIds(List<String> userIds) { this.userIds = new ArrayList<>(userIds); }
    public Map<String, String> getTags() { return tags; }
    public void setTags(Map<String, String> tags) { this.tags = new LinkedHashMap<>(tags); }
}
