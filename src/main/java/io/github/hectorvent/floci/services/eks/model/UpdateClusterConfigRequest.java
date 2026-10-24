package io.github.hectorvent.floci.services.eks.model;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import io.quarkus.runtime.annotations.RegisterForReflection;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * The body of {@code UpdateClusterConfig}. Floci changes {@code logging} only, so it keeps the names
 * of the other members a request sets, to refuse them.
 */
@RegisterForReflection
public class UpdateClusterConfigRequest {

    private Logging logging;
    private String clientRequestToken;
    private final Set<String> otherMembers = new LinkedHashSet<>();

    public Logging getLogging() { return logging; }
    public void setLogging(Logging logging) { this.logging = logging; }

    public String getClientRequestToken() { return clientRequestToken; }
    public void setClientRequestToken(String clientRequestToken) { this.clientRequestToken = clientRequestToken; }

    /** The members other than {@code logging} and {@code clientRequestToken} that the request set. */
    public Set<String> otherMembers() { return otherMembers; }

    @JsonAnySetter
    void setOtherMember(String name, Object value) {
        if (value != null) {
            otherMembers.add(name);
        }
    }
}
