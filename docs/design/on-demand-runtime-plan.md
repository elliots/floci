# On-demand application runtime implementation plan

## Objective

Add an opt-in runtime facility to Floci for explicitly configured Kubernetes deployments and
Docker-backed EC2 instances. Ordinary application HTTP requests and available SQS messages
activate a workload. Workloads stop after an idle interval, defaulting to 120 seconds.
Kubernetes targets can name a Floci EKS cluster, using its own k3s control plane and internally
resolved credentials, or use an existing cluster through Floci's Kubernetes client.
Existing enterprise tooling can continue deploying applications and generating explicit workload
configuration. Floci owns host-based gateway routing, activation, readiness, and idle shutdown.
Deployment configuration must make application names resolve to the always-on gateway, including
internal calls; the backend Service/container address remains separate and is used only by Floci.

## Boundaries and compatibility

- Keep AWS management protocols and port 4566 unchanged. Application traffic uses a separately
  configured HTTP gateway with stable host-based routes.
- Disable the feature by default. Read an externally generated YAML or JSON on-demand configuration file.
- Route external and internal application calls through the gateway. Direct pod/container
  connections cannot trigger activation. Backend URLs must bypass the gateway.
- Keep Kubernetes Services, volumes, Floci, and databases alive. Scale deployments to zero and
  stop EC2 containers through the existing service lifecycle, preserving their data.
- Treat configured workloads as owned by this runtime. External autoscalers must not compete.
  Disabling a workload in configuration prevents activation. EC2 states remain truthful.
- Explicit health method/path matches return configured synthetic responses only for deliberately
  dormant workloads. Running health probes reach the application and do not reset idle time.
  Readiness always checks the actual backend. Startup failures are never reported as healthy.
- Keep definitions in the external file and reconstruct runtime state on restart. Do not create
  a second persistent copy of AWS resources or replace StorageFactory-backed stores.

## Implementation steps

- [x] Add configuration defaults to EmulatorConfig and both application.yml files; implement a
  strict, reflection-safe on-demand configuration loader with route and runtime validation.
- [x] Implement a shared activation coordinator: one startup per workload, readiness deadlines,
  request leases, queue holds, idle shutdown, requests arriving during shutdown, bounded pending
  requests/startups, failure recovery, shutdown/reset hooks, and restart reconciliation.
- [x] Add an EC2 adapter using account/region scopes and existing start/stop methods; resolve
  backend addresses after every start. Add a Kubernetes deployment scale/readiness adapter by
  extending the existing lightweight Kubernetes API client. Resolve named Floci EKS clusters by
  account and region, share authenticated clients, and keep the k3s control plane running.
- [x] Add a separate Vert.x application gateway. Preserve HTTP methods, raw query strings,
  headers, request/response bodies, status codes, streaming/backpressure, and cancellation.
  Support WebSocket upgrades with leases held for the lifetime of each connection. Match health
  requests exactly, without waking workloads or counting them as application activity.
- [x] Reconcile configured SQS queues using visible and in-flight counts in the correct
  account/region. Add an asynchronous enqueue notification for fast activation, retaining
  periodic reconciliation for delays, retries, redrive, and restart. Never consume messages or
  change successful AWS send responses because activation failed.
- [x] Add user documentation, a generated-tooling configuration example, and navigation.
- [x] Verify lifecycle races with deterministic unit tests; exercise HTTP gateway behavior and
  runtime adapters against real local HTTP servers; cover SQS activation through AWS SDK clients
  on both Query and JSON paths. Run Checkstyle, partition-check, and docs-check.

## Acceptance criteria

1. A cold application request waits for real readiness and reaches the backend unchanged.
2. Simultaneous cold requests start one workload. Active requests, streams, and queue work prevent
   shutdown. Health probes do not extend idle time or start a sleeping workload.
3. Visible messages activate consumers; in-flight messages hold them awake; delayed messages and
   expired visibility become triggers through reconciliation. Consumers retain normal AWS semantics.
4. New requests during stop wait for a subsequent start. Failed readiness returns an error,
   releases leases, and permits a later retry. Health never hides a failed startup.
5. External configuration can select account, region, queues, Kubernetes deployment, EC2 instance,
   backend, HTTP host, exact health response, and readiness behavior without custom AWS APIs.
6. Default-disabled Floci behavior remains covered by existing tests.

## Validation constraints

Docker was initially unavailable and became available after the user started OrbStack. Use local
HTTP fakes for deterministic runtime and Kubernetes wire-contract tests, AWS SDK integration tests
for queue protocol behavior, and Docker-gated smoke tests for real EC2 containers and Floci-managed
k3s deployments. Native-image execution remains a separate deployment validation.

## Execution record

Implemented on branch `feat/on-demand-runtime`. The feature is disabled by default.

- Added strict on-demand configuration, a shared activation coordinator, bounded startup/request
  capacity, real backend readiness, activity leases, queue holds, and idle stop/start behavior.
- Added the separate HTTP/WebSocket gateway and exact health exclusions. Backend requests keep
  the original application Host header and request body; running health failures remain visible.
- Added scoped EC2 lifecycle integration and Kubernetes deployment scaling through the existing
  client. Named Floci EKS clusters resolve endpoints and k3s credentials internally. Kubernetes
  client keys use the existing shared PEM parser, including k3s's SEC1 keys.
- Added enqueue notifications without changing SQS responses. Reconciliation uses existing SQS
stores and does not consume messages. SDK tests cover delayed and in-flight messages, failures,
  account/region isolation, and the existing emulator reset endpoint.
- Quiesced activation before container teardown during reset. Interrupted EC2 activation drains
  the accepted background lifecycle operation before resource records can be cleared.
- Followed existing test patterns: JUnit HTTP fakes for client contracts, Quarkus profiles and
  alternatives for integration tests, and Docker-gated setup/cleanup for real runtime tests.
- Installed Homebrew `openjdk@25` alongside Java 26. Validation uses Java 25 through `JAVA_HOME`;
  removing Java 26 was unnecessary.

The selected regression suite passed **864 tests, with no failures, errors, or skips**. It covers
all new runtime tests and existing SQS, SNS, EC2, EKS, Kubernetes client/launcher, network-exposure,
and emulator lifecycle tests. The real Docker smoke tests verified:

1. An EC2 application's container starts, stops after idle, starts again, retains its filesystem,
   and exposes truthful instance states and a current container address.
2. A Floci-created k3s cluster authenticates the scaler using internally resolved credentials,
   scales a deployment from zero to available replicas and back to zero, and keeps the Kubernetes
   control plane running. No external Kubernetes cluster or kubeconfig changes are required.

Reproduce the regression selection with Java 25:

```bash
./mvnw test -Dtest='OnDemandEksDockerIntegrationTest,OnDemandEc2DockerIntegrationTest,OnDemandQueueIntegrationTest,ActivationCoordinatorServiceTest,OnDemandGatewayIntegrationTest,HttpReadinessServiceTest,OnDemandConfigLoaderServiceTest,WorkloadRuntimeFactoryServiceTest,EmulatorInfoControllerTest,EmulatorInfoControllerIntegrationTest,KubernetesDeploymentScaleTest,KubernetesApiClientKubeconfigTest,EksClusterManagerTest,EksServiceTest,SqsServiceTest,SqsIntegrationTest,SnsServiceTest,Ec2ServiceTest,NetworkExposureGuardTest,KubernetesPodLauncherTest'
./mvnw checkstyle:check
make partition-check docs-check
```

The Python repository checks require `tools/docs/requirements.txt` and
`tools/partition/requirements.txt`. They were installed into a temporary virtual environment on
this machine rather than changing the system Python environment.

Final checks passed: Checkstyle reported zero violations, partition literals matched the empty
baseline, `make docs-check` succeeded, and `git diff --check` was clean. After aligning Docker
fixture setup/cleanup with nearby tests, a final rerun passed all seven EC2, EKS, and queue
integration cases without skips.

A complete 300-application deployment and customer-facing cloud ingress remain deployment
validations. This implementation does not suspend databases, forward raw TCP,
install DNS rules, or provide distributed activation ownership. Those boundaries
are documented in `docs/configuration/on-demand-runtime.md`.

## Packaged application validation

Built the JVM package using Java 25 and the regression selection above. All 864 tests passed,
including real Docker-backed EC2 and Floci-owned k3s tests, and Checkstyle reported zero violations.
Ran the packaged `quarkus-run.jar` as a separate process with the on-demand feature enabled.

Added opt-in live tests under `compatibility-tests/sdk-test-python`, using the module's existing
boto3/pytest conventions. The tests provision their own k3s cluster through EKS and deploy a real
HTTP application at zero replicas. All four live cases passed against the packaged JVM process:

- Sleeping health probes leave the deployment at zero.
- Twelve simultaneous cold requests reach one pod, preserving binary bodies, query strings,
  the application Host header, and authorization. Running health failures remain visible.
- A streaming response lasting six seconds holds the pod beyond the three-second test idle interval.
- Delayed SQS messages activate only when visible, remain available for the application to receive,
  and keep the deployment awake while in flight. Acknowledgement permits scale-down, while the
  k3s control plane remains running.

The existing Python SQS/SNS compatibility tests also passed: 26 tests against the packaged process.
The live tests exposed and fixed two lifecycle races, with added Java regression coverage:

1. Failed initial inspection remains unknown and retries as deployment provisioning completes.
   Actual startup failures still report failure.
2. Kubernetes shutdown waits for matching pods to disappear, including terminating pods and
   clusters without the newer `terminatingReplicas` status field, before allowing a subsequent start.

Reproduction commands and fixture configuration are in `compatibility-tests/sdk-test-python/README.md`.
Live tests deliberately shorten idle time to three seconds; configuration tests verify the
120-second default. The full repository test suite was not run, only the relevant regression selection.

Built the Linux ARM64 native executable with `make native`, using Mandrel 25.0.4.1 and the
Makefile's CI flags. Packaged it with `make native-image NATIVE_IMAGE=floci:on-demand-native`.
Ran that image with the Docker socket, the on-demand configuration file, and a dedicated Docker network.
The same four live on-demand tests and all 26 existing SQS/SNS compatibility tests passed against
the native container: **30 tests, with no failures or skips**. The native run created its own k3s
container, used its internally resolved credentials, and connected to application pods through
the backend NodePort on the Docker network.

Temporary test processes, containers, and the dedicated network were removed. The JVM package
and test reports are retained under `/tmp/floci-on-demand-live`; the native artifacts remain in
`native/arm64` and the local image remains tagged `floci:on-demand-native`.


## ECS and HTTP/2 follow-up

Extend the existing runtime without changing AWS management endpoint shapes or deployment ownership.

- [x] Add strict ECS configuration for a service, cluster, awake replica count, and optional stable
  backend URL. Reuse account/region request scopes and the existing desired-count scheduler.
- [x] Wait for current-deployment task readiness and configured container health checks; wait for
  every owned task, including STOPPING tasks, to drain before a later activation.
- [x] Honor ECS task scale-in protection and expiry in both idle detection and scheduler scale-in.
- [x] Accept h2c and select backend HTTP/2 explicitly, preserving authority, gRPC frames, metadata,
  response trailers, streaming, and cancellation without closing sibling HTTP/2 streams.
- [x] Add configuration/lifecycle/protocol regression tests and a real Docker-backed ECS test.
- [x] Add opt-in boto3 and grpcio tests in the existing Python SDK module, covering all RPC modes,
  concurrent cold calls, binary metadata/compression/status details, task protection, SQS, and deadlines.
- [x] Run selected Java regressions, Checkstyle, partition/doc checks, and packaged JVM/native tests.

ECS HTTP routes require an explicit stable backend, avoiding a cached task IP or dynamic port.
The ECS adapter controls Floci resources. Actual AWS ECS services, DAEMON/EXTERNAL/CODE_DEPLOY services,
cloud ingress/DNS generation, and a generic database or raw TCP scaler remain outside this change.


Packaged ECS validation exposed an existing task-health gap: task-definition health checks were
stored and returned by the AWS APIs but not installed on Docker containers. Added optional health
checks to the shared container specification/builder/lifecycle path, preserving older constructors
and existing image defaults when no check is supplied. ECS now installs declared commands with
ECS timing defaults and aggregates only essential declared checks, including UNKNOWN priority.
Added Docker wire-contract and task-health regression coverage.

HTTP/2 error handling resets a failed response stream instead of closing the underlying connection;
regression coverage verifies an active sibling stream still completes after an upstream reset.

Open-upload cancellation also exposed an event-loop cleanup error when resuming an already-ended
HTTP/2 request. Drain requests only while they remain readable. The gateway regression suite now
asserts there are no unhandled Vert.x exceptions, including during cancellation and stream resets.

Selected Java regression validation passed 1,018 tests covering on-demand activation, ECS,
container lifecycle, SQS, and emulator information. Both real Docker ECS launch-type cases,
EC2 and Fargate, ran without skips. After the final cancellation fix, another 19 focused gateway
and health-check tests passed. Retained reports cover 1,021 distinct selected tests with no
failures, errors, or skips. Checkstyle reported zero violations, and the partition and documentation
checks passed. The full repository test suite was not run.

The final packaged JVM passed the four new boto3/grpcio cases and all 26 existing SQS/SNS
compatibility tests: 30 passed with no skips. The real gRPC server ran in an ECS-managed container
with a task-definition health check, which Docker inspection confirmed was installed and healthy.
No uncaught proxy errors appeared in the final run. JVM packages, Java reports, and live test logs
are retained under `/tmp/floci-ecs-grpc-live`.

Built the Linux ARM64 native executable with `make native`, using Mandrel 25.0.4.1 and the
repository's CI flags. Packaged it as the local image `floci:ecs-grpc-native`. All four new ECS/gRPC
SDK cases and all 26 existing SQS/SNS compatibility tests also passed against this native image:
30 passed with no skips. Real container health checks, all four gRPC call types, cancellation,
task protection, delayed queue activation, in-flight holds, and return to zero worked in both builds.
No uncaught proxy errors appeared in the native run.

Removed the test processes, native test container, its anonymous data volume, and the dedicated
Docker network. Test workloads cleaned up their own AWS resources and ECS containers. Native
artifacts remain under `native/arm64`, the local native and gRPC fixture images remain cached, and
the final native logs and SDK reports are retained under `/tmp/floci-ecs-grpc-live`.

All eight on-demand compatibility cases now live in
`compatibility-tests/sdk-test-python/tests/test_on_demand_runtime.py`, sharing one gateway fixture,
the `FLOCI_ON_DEMAND_TEST_GATEWAY` opt-in, and `fixtures/on-demand.yaml`. Kubernetes and
ECS have separate resource setup/cleanup fixtures within that module. Removed the separate ECS
test module, on-demand configuration file, and gateway opt-in; the README provides one set of JVM/native commands.

Validated collection of all eight cases and their opt-out behavior. Ran the combined suite against
the current native image: all eight on-demand cases and 26 existing SQS/SNS compatibility cases
passed together, 34 tests with no failures or skips. Documentation checks passed. Test resources,
the native test container and its anonymous data volume, and the dedicated network were removed.
Reports and logs are retained under `/tmp/floci-on-demand-combined`.
