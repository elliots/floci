package io.github.hectorvent.floci.services.eks;

import io.github.hectorvent.floci.services.eks.model.Cluster;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/**
 * The token and Pod Identity admission webhooks of the smolvm-pods node's k3s. The node's seed
 * names these routes, which belong to the node rather than to a cluster, since the node's k3s
 * runs before and after its cluster: each call goes to the routes of the cluster that runs on
 * the node now. Without one, a token is not authenticated and a pod is admitted as it is.
 *
 * <p>This is Floci plumbing under the {@code _floci/...} namespace, not an AWS API.
 */
@ApplicationScoped
@Path(EksSmolvmNodeWebhookController.BASE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class EksSmolvmNodeWebhookController {

    static final String BASE_PATH = "_floci/eks/smolvm-node";
    static final String TOKEN_WEBHOOK_PATH = "/" + BASE_PATH + "/token-webhook";
    static final String POD_IDENTITY_WEBHOOK_PATH = "/" + BASE_PATH + "/pod-identity-webhook";

    private final EksSmolvmCluster smolvmCluster;
    private final EksTokenWebhookController tokenWebhook;
    private final EksPodIdentityWebhook podIdentityWebhook;

    @Inject
    public EksSmolvmNodeWebhookController(EksSmolvmCluster smolvmCluster, EksTokenWebhookController tokenWebhook,
                                          EksPodIdentityWebhook podIdentityWebhook) {
        this.smolvmCluster = smolvmCluster;
        this.tokenWebhook = tokenWebhook;
        this.podIdentityWebhook = podIdentityWebhook;
    }

    @POST
    @Path("token-webhook")
    public Response tokenReview(Map<String, Object> tokenReview) {
        Optional<Cluster> cluster = smolvmCluster.cluster();
        if (cluster.isEmpty()) {
            String apiVersion = tokenReview != null && tokenReview.get("apiVersion") instanceof String v
                    ? v : "authentication.k8s.io/v1";
            return Response.ok(Map.of("apiVersion", apiVersion, "kind", "TokenReview",
                    "status", Map.of("authenticated", false))).build();
        }
        Cluster current = cluster.get();
        Instant createdAt = current.getCreatedAt() != null ? current.getCreatedAt() : Instant.EPOCH;
        return tokenWebhook.reviewScoped(current.getName(), current.getAccountId(), smolvmCluster.region(current),
                createdAt.toString(), tokenReview);
    }

    @POST
    @Path("pod-identity-webhook")
    public Response admissionReview(Map<String, Object> admissionReview) {
        Optional<Cluster> cluster = smolvmCluster.cluster();
        String name = cluster.map(Cluster::getName).orElse("");
        String accountId = cluster.map(Cluster::getAccountId).orElse("");
        return Response.ok(podIdentityWebhook.review(name, accountId, admissionReview)).build();
    }
}
