package io.github.hectorvent.floci.services.lambda.launcher;

import com.github.dockerjava.api.DockerClient;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.dns.EmbeddedDnsServer;
import io.github.hectorvent.floci.core.common.docker.ContainerBuilder;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager;
import io.github.hectorvent.floci.core.common.docker.ContainerLogStreamer;
import io.github.hectorvent.floci.core.common.docker.ContainerReachableEndpoint;
import io.github.hectorvent.floci.core.common.docker.ContainerSpec;
import io.github.hectorvent.floci.core.common.docker.DockerHostResolver;
import io.github.hectorvent.floci.core.common.docker.LaunchedContainerAwsEnv;
import io.github.hectorvent.floci.runtime.node.NodeDocker;
import io.github.hectorvent.floci.runtime.node.NodeToken;
import io.github.hectorvent.floci.runtime.node.SmolvmNode;
import io.github.hectorvent.floci.services.ecr.registry.EcrRegistryManager;
import io.github.hectorvent.floci.services.lambda.LambdaLayerService;
import io.github.hectorvent.floci.services.lambda.model.LambdaFunction;
import io.github.hectorvent.floci.services.lambda.runtime.RuntimeApiServer;
import io.github.hectorvent.floci.services.lambda.runtime.RuntimeApiServerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ContainerLauncherSmolvmTest {

    private static final String IMAGE = "public.ecr.aws/lambda/nodejs:20";

    private final EmulatorConfig config = mock(EmulatorConfig.class, RETURNS_DEEP_STUBS);
    private final ContainerLifecycleManager localLifecycle = mock(ContainerLifecycleManager.class);
    private final ContainerLifecycleManager nodeLifecycle = mock(ContainerLifecycleManager.class);
    private final ContainerLogStreamer nodeLogs = mock(ContainerLogStreamer.class);
    private final DockerHostResolver dockerHostResolver = mock(DockerHostResolver.class);
    private final RuntimeApiServerFactory runtimeApiServers = mock(RuntimeApiServerFactory.class);
    private final RuntimeApiServer runtimeApiServer = mock(RuntimeApiServer.class);
    private final LambdaSmolvmBackend smolvm = mock(LambdaSmolvmBackend.class);
    private LambdaSmolvmBackend.Target target;
    private ContainerLauncher launcher;

    @BeforeEach
    void setUp() {
        when(config.services().lambda().dockerNetwork()).thenReturn(Optional.of("floci-net"));
        when(config.services().lambda().extraHosts()).thenReturn(Optional.empty());
        when(config.services().lambda().awsConfigPath()).thenReturn(Optional.empty());
        when(config.services().lambda().dockerFlags()).thenReturn(Optional.empty());
        when(config.services().lambda().containerNamePrefix()).thenReturn(Optional.empty());
        when(config.services().lambda().codeVolumePopulateConcurrency()).thenReturn(Optional.empty());
        when(config.services().lambda().honourArchitectures()).thenReturn(false);
        when(config.docker().resourceNamespace()).thenReturn(Optional.empty());
        when(config.docker().logMaxSize()).thenReturn("10m");
        when(config.docker().logMaxFile()).thenReturn("3");
        when(config.defaultRegion()).thenReturn("us-east-1");
        when(config.defaultAccountId()).thenReturn("000000000000");
        when(config.hostname()).thenReturn(Optional.empty());
        when(config.tls().enabled()).thenReturn(false);

        EmbeddedDnsServer dns = mock(EmbeddedDnsServer.class);
        when(dns.getServerIp()).thenReturn(Optional.of("172.30.0.53"));
        ContainerReachableEndpoint endpoint = new ContainerReachableEndpoint(config, dockerHostResolver, dns, 4566);
        EcrRegistryManager ecr = mock(EcrRegistryManager.class);
        when(ecr.rewriteImageUri(any())).thenAnswer(inv -> inv.getArgument(0));
        LambdaExecutionRoleCredentials roles = mock(LambdaExecutionRoleCredentials.class);
        when(roles.forFunction(any())).thenReturn(Optional.empty());

        DockerClient nodeDocker = mock(DockerClient.class);
        SmolvmNode node = new SmolvmNode("floci", URI.create("http://127.0.0.1:6600"),
                NodeToken.file(Path.of("token")), SmolvmNode.Kind.CREATED, List.of());
        target = new LambdaSmolvmBackend.Target(node,
                new NodeDocker.Clients(node.baseUrl(), node.token(), nodeDocker, nodeDocker), nodeLifecycle, nodeLogs);
        when(smolvm.enabled()).thenReturn(true);
        when(smolvm.target(anyString(), anyString())).thenReturn(target);
        when(smolvm.runtimeApiHost()).thenReturn("169.254.53.1");
        when(smolvm.flociEndpoint()).thenReturn("http://localhost.floci.io:4566");
        when(smolvm.ownerLabels()).thenReturn(Map.of("io.floci.owner", "4566", "io.floci.service", "lambda"));

        when(runtimeApiServers.create()).thenReturn(runtimeApiServer);
        when(runtimeApiServer.getPort()).thenReturn(12000);
        when(runtimeApiServer.close()).thenReturn(CompletableFuture.completedFuture(null));
        when(nodeLifecycle.create(any())).thenReturn("c0ffee");
        when(nodeLifecycle.getDockerClient()).thenReturn(nodeDocker);

        launcher = new ContainerLauncher(new ContainerBuilder(config, dockerHostResolver, dns), localLifecycle,
                mock(ContainerLogStreamer.class), mock(ImageResolver.class), runtimeApiServers, dockerHostResolver,
                config, ecr, mock(LambdaLayerService.class), new LaunchedContainerAwsEnv(endpoint), roles, smolvm);
    }

    private static LambdaFunction imageFunction() {
        LambdaFunction fn = new LambdaFunction();
        fn.setFunctionName("orders");
        fn.setFunctionArn("arn:aws:lambda:us-east-1:000000000000:function:orders");
        fn.setPackageType("Image");
        fn.setImageUri(IMAGE);
        return fn;
    }

    @Test
    void anEnvironmentRunsOnTheNodeWithThePodsNetwork() {
        ContainerHandle handle = launcher.launch(imageFunction());

        ArgumentCaptor<ContainerSpec> spec = ArgumentCaptor.forClass(ContainerSpec.class);
        verify(nodeLifecycle).create(spec.capture());
        verify(nodeLifecycle).startCreated(eq("c0ffee"), any());
        verify(localLifecycle, never()).create(any());
        verify(smolvm).ensureImage(target, IMAGE, null);
        assertThat(spec.getValue().env(), hasItem("AWS_LAMBDA_RUNTIME_API=169.254.53.1:12000"));
        assertThat(spec.getValue().env(), hasItem("AWS_ENDPOINT_URL=http://localhost.floci.io:4566"));
        assertThat(spec.getValue().networkMode(), nullValue());
        assertThat(spec.getValue().dnsServers(), empty());
        assertThat(spec.getValue().extraHosts(), not(hasItem(containsString("host.docker.internal"))));
        assertThat(spec.getValue().labels(), hasEntry("io.floci.owner", "4566"));
        assertThat(spec.getValue().labels(), hasEntry("io.floci.resource-id", "orders"));

        LambdaRuntimeLauncher.Hold hold = () -> { };
        when(smolvm.hold(target, "c0ffee", "orders")).thenReturn(hold);
        assertSame(hold, launcher.hold(handle));

        when(nodeLifecycle.isContainerRunning("c0ffee")).thenReturn(true);
        launcher.isAlive(handle);
        verify(nodeLifecycle).isContainerRunning("c0ffee");
        launcher.stop(handle);
        verify(nodeLifecycle).stopAndRemoveStrict(eq("c0ffee"), isNull(), any(Integer.class));
        verify(localLifecycle, never()).stopAndRemoveStrict(anyString(), any(), any(Integer.class));
    }

    @Test
    void hotReloadIsRefusedOnTheNode() {
        LambdaFunction fn = imageFunction();
        fn.setHotReloadHostPath("/Users/me/code");

        RuntimeException refused = assertThrows(RuntimeException.class, () -> launcher.launch(fn));

        assertThat(refused.getMessage(), containsString("Hot reload"));
        verify(runtimeApiServers, never()).create();
    }

    @Test
    void anEnvironmentInFlociDockerNeedsNoHold() {
        when(smolvm.enabled()).thenReturn(false);
        when(localLifecycle.create(any())).thenReturn("local");
        when(localLifecycle.getDockerClient()).thenReturn(mock(DockerClient.class));
        when(dockerHostResolver.resolve()).thenReturn("172.30.0.1");

        ContainerHandle handle = launcher.launch(imageFunction());

        launcher.hold(handle).close();
        verify(smolvm, never()).hold(any(), anyString(), anyString());
        verify(smolvm, never()).target(anyString(), anyString());
        ArgumentCaptor<ContainerSpec> spec = ArgumentCaptor.forClass(ContainerSpec.class);
        verify(localLifecycle).create(spec.capture());
        assertThat(spec.getValue().networkMode(), is("floci-net"));
        assertThat(spec.getValue().labels(), not(hasEntry("io.floci.owner", "4566")));
        assertThat(spec.getValue().env(), hasItem("AWS_LAMBDA_RUNTIME_API=172.30.0.1:12000"));
    }
}
