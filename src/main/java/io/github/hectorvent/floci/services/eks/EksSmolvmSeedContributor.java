package io.github.hectorvent.floci.services.eks;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.config.EmulatorConfig.EksServiceConfig;
import io.github.hectorvent.floci.config.FlociCertificateAuthority;
import io.github.hectorvent.floci.runtime.flocinode.NodeRecord;
import io.github.hectorvent.floci.runtime.flocinode.NodeSeed;
import io.github.hectorvent.floci.runtime.flocinode.NodeSeedContributor;
import io.github.hectorvent.floci.runtime.flocinode.NodeSettings;
import io.github.hectorvent.floci.runtime.node.SmolvmNode;
import io.github.hectorvent.floci.services.eks.EksClusterArgs.Component;
import io.github.hectorvent.floci.services.eks.model.ClusterOidcKey;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * What EKS adds to the seed of the smolvm-pods node, so that the node's k3s is ready for the one
 * cluster that runs on it, whenever that cluster is created: the token webhook and the Pod Identity
 * admission webhook, at routes of the node rather than of a cluster
 * ({@link EksSmolvmNodeWebhookController}), the node's service account signing key and issuer,
 * which the cluster adopts, and the ECR mirror. Only when new EKS clusters are real smolvm-pods
 * clusters.
 */
@ApplicationScoped
public class EksSmolvmSeedContributor implements NodeSeedContributor {

    private static final Logger LOG = Logger.getLogger(EksSmolvmSeedContributor.class);

    /** The name that {@link NodeRecord#seeded()} lists for a node whose k3s has EKS's additions. */
    public static final String NAME = "eks";

    /**
     * The API server always writes its audit log with the Amazon EKS policy, whether or not the
     * cluster ships the {@code audit} type, so that turning it on needs no restart. It keeps three
     * files of 50 MB.
     */
    static final List<String> AUDIT_ARGS = List.of(
            "audit-policy-file=" + NodeSeed.NODE_AUDIT_POLICY,
            "audit-log-path=" + NodeSeed.NODE_AUDIT_LOG,
            "audit-log-maxsize=50",
            "audit-log-maxbackup=2");

    private final EmulatorConfig config;
    private final NodeSettings settings;
    private final EksOidcService oidcService;
    private final FlociCertificateAuthority certificateAuthority;

    @Inject
    public EksSmolvmSeedContributor(EmulatorConfig config, NodeSettings settings, EksOidcService oidcService,
                                    FlociCertificateAuthority certificateAuthority) {
        this.config = config;
        this.settings = settings;
        this.oidcService = oidcService;
        this.certificateAuthority = certificateAuthority;
    }

    /** Whether new EKS clusters run on the node. */
    boolean active() {
        EksServiceConfig eks = config.services().eks();
        return eks.enabled() && !eks.mock() && EksSmolvmSettings.smolvmProvider(config);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Contribution contribute(NodeRecord node) {
        if (!active()) {
            return Contribution.NONE;
        }
        EksServiceConfig eks = config.services().eks();
        String endpoint = settings.flociEndpoint();
        Map<String, String> files = new LinkedHashMap<>();
        List<String> apiServerArgs = new ArrayList<>();
        if (eks.iamAuthWebhook()) {
            files.put(NodeSeed.TOKEN_WEBHOOK_FILE, EksClusterManager.buildWebhookKubeconfig(
                    "http://" + endpoint + EksSmolvmNodeWebhookController.TOKEN_WEBHOOK_PATH));
            apiServerArgs.addAll(componentArgs(Component.KUBE_APISERVER,
                    EksClusterManager.buildTokenWebhookServerArgs(NodeSeed.NODE_TOKEN_WEBHOOK)));
        }
        if (eks.irsaSigningKey()) {
            ClusterOidcKey key = oidcService.nodeKey(node.accountId(), node.nodeId(), node.region());
            files.put(NodeSeed.SIGNING_KEY_FILE, oidcService.exportSigningKeyPem(key));
            files.put(NodeSeed.PUBLIC_KEY_FILE, oidcService.exportPublicKeyPem(key));
            apiServerArgs.addAll(componentArgs(Component.KUBE_APISERVER, EksClusterManager.buildIrsaServerArgs(
                    key.getIssuer(), NodeSeed.NODE_SIGNING_KEY, NodeSeed.NODE_PUBLIC_KEY)));
        }
        if (eks.ecrRegistryMirror() && config.services().ecr().enabled()) {
            files.put(NodeSeed.REGISTRIES_FILE, registries(endpoint));
        }
        files.put(NodeSeed.AUDIT_POLICY_FILE, EksClusterManager.buildAuditPolicy());
        apiServerArgs.addAll(AUDIT_ARGS);
        String admission = admissionManifest(endpoint);
        if (admission != null) {
            files.put(NodeSeed.ADMISSION_FILE, admission);
        }
        return new Contribution(files, Map.of(NodeSeed.API_SERVER_ARGS, apiServerArgs));
    }

    /** Pod Identity's link-local endpoint, when Floci serves the admission webhook. */
    @Override
    public List<SmolvmNode.Endpoint> endpoints(NodeRecord node) {
        if (!active() || !config.services().eks().podIdentityWebhook() || !config.tls().enabled()) {
            return List.of();
        }
        LinkLocalEndpoint podIdentity = EksPodNetworkRouting.POD_IDENTITY_ENDPOINT;
        return List.of(new SmolvmNode.Endpoint(podIdentity.ip(), podIdentity.port(), config.port()));
    }

    /**
     * The k3s settings of the arguments {@code callerArgs} of a cluster, by setting, without their
     * {@code --*-arg=} prefix.
     */
    static Map<String, List<String>> k3sArgs(List<String> callerArgs) {
        Map<String, List<String>> args = new LinkedHashMap<>();
        putArgs(args, NodeSeed.KUBELET_ARGS, componentArgs(Component.KUBELET, callerArgs));
        putArgs(args, NodeSeed.API_SERVER_ARGS, componentArgs(Component.KUBE_APISERVER, callerArgs));
        putArgs(args, NodeSeed.CONTROLLER_MANAGER_ARGS, componentArgs(Component.KUBE_CONTROLLER_MANAGER, callerArgs));
        putArgs(args, NodeSeed.SCHEDULER_ARGS, componentArgs(Component.KUBE_SCHEDULER, callerArgs));
        putArgs(args, NodeSeed.CLOUD_CONTROLLER_MANAGER_ARGS,
                componentArgs(Component.KUBE_CLOUD_CONTROLLER_MANAGER, callerArgs));
        return args;
    }

    private static void putArgs(Map<String, List<String>> into, String setting, List<String> args) {
        if (!args.isEmpty()) {
            into.put(setting, args);
        }
    }

    /** The values of {@code serverArgs} for {@code component}, without its {@code --*-arg=} prefix. */
    private static List<String> componentArgs(Component component, List<String> serverArgs) {
        List<String> args = new ArrayList<>();
        for (String arg : serverArgs) {
            if (arg.startsWith(component.flagPrefix())) {
                args.add(arg.substring(component.flagPrefix().length()));
            }
        }
        return args;
    }

    /**
     * Every registry mirrored to Floci, which serves the ECR repositories of every account and
     * region by the registry that containerd names, and answers 404 for any other registry: k3s
     * takes no partial wildcard, and the accounts of the clusters that the node holds over its life
     * are not known when it is created.
     */
    private static String registries(String endpoint) {
        StringBuilder yaml = new StringBuilder("mirrors:\n");
        EksClusterManager.appendMirror(yaml, "*", "http://" + endpoint);
        return yaml.toString();
    }

    /**
     * The admission webhook that Pod Identity uses, or null when it is off. Kubernetes calls an
     * admission webhook only over https, so it needs Floci's TLS.
     */
    private String admissionManifest(String endpoint) {
        if (!config.services().eks().podIdentityWebhook()) {
            return null;
        }
        if (!config.tls().enabled() || certificateAuthority == null) {
            LOG.warnv("The smolvm-pods node gets no admission webhook: Kubernetes calls one only over https. "
                    + "Set FLOCI_TLS_ENABLED=true to have pods mutated.");
            return null;
        }
        return EksClusterManager.buildPodIdentityWebhookConfiguration(
                "https://" + endpoint + EksSmolvmNodeWebhookController.POD_IDENTITY_WEBHOOK_PATH,
                certificateAuthority.caPem());
    }
}
