# sdk-test-python

Compatibility tests for [Floci](https://github.com/hectorvent/floci) using **boto3 (1.37.1)**.

## Services Covered

| Group                   | Description                                                              |
| ----------------------- | ------------------------------------------------------------------------ |
| `ssm`                   | Parameter Store — put, get, label, history, path, tags                   |
| `sqs`                   | Queues, send/receive/delete, DLQ, visibility                             |
| `sns`                   | Topics, subscriptions, publish, SQS delivery                             |
| `s3`                    | Buckets, objects, tagging, copy, batch delete                            |
| `s3-cors`               | CORS configuration                                                       |
| `s3-notifications`      | S3 → SQS event notifications                                             |
| `dynamodb`              | Tables, CRUD, batch, TTL, tags                                           |
| `lambda`                | Create/invoke/update/delete functions                                    |
| `iam`                   | Users, roles, policies, access keys                                      |
| `sts`                   | GetCallerIdentity, AssumeRole, GetSessionToken                           |
| `secretsmanager`        | Create/get/put/list/delete secrets, versioning, tags                     |
| `kms`                   | Keys, aliases, encrypt/decrypt, data keys, sign/verify                   |
| `kinesis`               | Streams, shards, PutRecord/GetRecords                                    |
| `cloudwatch-metrics`    | PutMetricData, ListMetrics, GetMetricStatistics, alarms                  |
| `cloudformation-naming` | Auto physical name generation, explicit name precedence, cross-reference |
| `cognito`               | User pools, clients, AdminCreateUser, InitiateAuth, GetUser              |

## Requirements

- Python 3.9+
- pip

## Running

```bash
pip install -r requirements.txt

# All groups
pytest tests/ --junit-xml=test-results/junit.xml

# Specific tests
pytest tests/test_s3.py

# Via just (from compatibility-tests/)
just test-python
```

## Configuration

| Variable         | Default                 | Description             |
| ---------------- | ----------------------- | ----------------------- |
| `FLOCI_ENDPOINT` | `http://localhost:4566` | Floci emulator endpoint |

AWS credentials are always `test` / `test` / `us-east-1`.

## Opt-in packaged on-demand runtime tests

`tests/test_on_demand_runtime.py` contains all eight on-demand compatibility cases, using one
packaged Floci process, one gateway, and `fixtures/on-demand.yaml`. Kubernetes/HTTP
cases use real pods in a Floci-created k3s cluster; ECS/gRPC cases use real ECS-managed Docker
containers. Both use boto3 for AWS management and SQS, and gRPC cases also use grpcio. Each runtime
fixture creates and deletes its own AWS resources. The gRPC test image remains cached locally.

Install `requirements-on-demand.txt` to include grpcio. Ordinary suite runs skip all eight cases
unless `FLOCI_ON_DEMAND_TEST_GATEWAY` is set. Use an isolated Floci instance and unique cluster names
when running concurrently. To debug individual cases, select test names with pytest's `-k` option.

From the repository root, build the JVM package with Java 25, then start it in one terminal:

```bash
./mvnw clean package -DskipTests
mkdir -p /tmp/floci-on-demand-live
# OrbStack makes container addresses reachable from a macOS host through .orb.local.
# On other hosts, choose an address that reaches the k3s NodePort from Floci's process.
sed 's/eks-on-demand-live:30080/eks-on-demand-live.orb.local:30080/' \
  compatibility-tests/sdk-test-python/tests/fixtures/on-demand.yaml \
  > /tmp/floci-on-demand-live/on-demand.yaml

FLOCI_PORT=14566 \
FLOCI_BASE_URL=http://127.0.0.1:14566 \
FLOCI_STORAGE_PERSISTENT_PATH=/tmp/floci-on-demand-live/data \
FLOCI_DOCKER_RESOURCE_NAMESPACE=on-demand-live \
FLOCI_SERVICES_EKS_DATA_PATH=/tmp/floci-on-demand-live/eks \
FLOCI_SERVICES_EKS_DEFAULT_IMAGE=rancher/k3s:v1.35.0-k3s1 \
FLOCI_SERVICES_EKS_ECR_REGISTRY_MIRROR=false \
FLOCI_SERVICES_EKS_EMBEDDED_DNS=false \
FLOCI_SERVICES_EKS_POD_IDENTITY_WEBHOOK=false \
FLOCI_ON_DEMAND_ENABLED=true \
FLOCI_ON_DEMAND_CONFIG_FILE=/tmp/floci-on-demand-live/on-demand.yaml \
FLOCI_ON_DEMAND_GATEWAY_PORT=18080 \
java --enable-native-access=ALL-UNNAMED -jar target/quarkus-app/quarkus-run.jar
```

In another terminal:

```bash
python -m pip install -r compatibility-tests/sdk-test-python/requirements-on-demand.txt
FLOCI_ENDPOINT=http://127.0.0.1:14566 \
FLOCI_ON_DEMAND_TEST_GATEWAY=http://127.0.0.1:18080 \
python -m pytest compatibility-tests/sdk-test-python/tests/test_on_demand_runtime.py
```

The fixtures use a three-second idle interval to keep repeated cycles quick. They verify
sleeping health probes, concurrent cold requests reaching one pod, binary bodies and request
headers, real running health failures, active requests lasting beyond idle, delayed SQS
activation, non-consumption by the scaler, in-flight holds, acknowledgement, and return to zero.
The k3s control plane must remain running after the application scales down. ECS/gRPC cases
verify compressed binary messages and metadata, trailers and errors, all four RPC types,
per-stream cancellation, task protection, fresh tasks after restart, SQS activity, and deadlines.
The ECS backend uses a fixed Docker host port 15051. Override
`FLOCI_ON_DEMAND_TEST_ECS_BACKEND_PORT` and the on-demand configuration file together if that port
is occupied.

The same tests can target the native Docker image. Stop the JVM process first, then run:

```bash
make native native-image NATIVE_IMAGE=floci:on-demand-native
mkdir -p /tmp/floci-on-demand-live
# The native container reaches the ECS task's published port through the Docker host.
sed 's/127.0.0.1:15051/host.docker.internal:15051/' \
  compatibility-tests/sdk-test-python/tests/fixtures/on-demand.yaml \
  > /tmp/floci-on-demand-live/native-on-demand.yaml
docker network create floci-on-demand-live
docker run -d --rm --name floci-on-demand-native-check \
  --network floci-on-demand-live \
  --add-host=host.docker.internal:host-gateway \
  -p 127.0.0.1:14566:4566 -p 127.0.0.1:18080:18080 \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v /tmp/floci-on-demand-live/native-on-demand.yaml:/app/on-demand.yaml:ro \
  -e FLOCI_BASE_URL=http://127.0.0.1:14566 \
  -e FLOCI_DOCKER_RESOURCE_NAMESPACE=on-demand-live \
  -e FLOCI_SERVICES_EKS_DOCKER_NETWORK=floci-on-demand-live \
  -e FLOCI_SERVICES_EKS_DEFAULT_IMAGE=rancher/k3s:v1.35.0-k3s1 \
  -e FLOCI_SERVICES_EKS_ECR_REGISTRY_MIRROR=false \
  -e FLOCI_SERVICES_EKS_EMBEDDED_DNS=false \
  -e FLOCI_SERVICES_EKS_POD_IDENTITY_WEBHOOK=false \
  -e FLOCI_ON_DEMAND_ENABLED=true \
  -e FLOCI_ON_DEMAND_CONFIG_FILE=/app/on-demand.yaml \
  -e FLOCI_ON_DEMAND_GATEWAY_HOST=0.0.0.0 \
  -e FLOCI_ON_DEMAND_GATEWAY_PORT=18080 \
  -e FLOCI_SECURITY_ALLOW_UNSAFE_NETWORK_EXPOSURE=true \
  floci:on-demand-native
```

Wait for `http://127.0.0.1:14566/_floci/health`, then run the pytest command above. To include
the existing SQS/SNS compatibility coverage, also select `tests/test_sqs.py` and
`tests/test_sns.py` in the same module. After testing, stop `floci-on-demand-native-check` and
remove the `floci-on-demand-live` network. The fixture's unmodified backend hostname reaches
the k3s NodePort over that shared network.

## Opt-in metric filter replay against AWS

`tests/metric_filter_replay.py` is a standalone script, not a test: pytest does
not collect it, and `tests/test_metric_filter_replay.py` checks its logic offline
with the network blocked. It does not change `conftest.py`, and `FLOCI_TARGET=aws`
does not select real AWS.

From the repository root, with the declared requirements installed:

```bash
python -m pytest compatibility-tests/sdk-test-python/tests/test_metric_filter_replay.py

# Against a local Floci
python compatibility-tests/sdk-test-python/tests/metric_filter_replay.py \
  --endpoint http://127.0.0.1:4566 --region eu-west-1 \
  --timeout 30 --stable-seconds 2 --poll-interval 1
```

Real AWS mode requires all three safety arguments. **It creates billable
CloudWatch metrics and a tagged log group, and metric series cannot be deleted**;
they remain until AWS ages them out.

```bash
python compatibility-tests/sdk-test-python/tests/metric_filter_replay.py \
  --aws --profile YOUR_DISPOSABLE_TEST_PROFILE --region eu-west-1 \
  --ack-live-writes I_ACCEPT_AWS_WRITES
```

Only commercial AWS regions are accepted, the clients pin the official regional
endpoints, and the group is deleted in `finally` only after its ownership tag is
verified. A cleanup failure exits nonzero and prints the group name. Credentials,
account ids, profile names and raw service errors are never printed.

The expected values come from `tests/fixtures/metric-filter-publishing-aws.json`,
a byte-identical copy of the root fixture
`src/test/resources/cloudwatchlogs/metric-filter-publishing-aws.json`; the root
`CloudWatchLogsMetricFilterFixturePackagingTest` fails when either SDK module copy
drifts. The replay ingests each scenario into its own backdated minute, runs the
quiet-default scenario last with no later ingestion, and accepts a result only
after every expected series, absent series and the idle minute after the quiet one
have stayed stable for a full window (AWS defaults: 240 second deadline, 30 second
window). Output is JSON lines of observations, not a regenerated fixture.

## Docker

```bash
docker build -t floci-sdk-python .
docker run --rm --network host floci-sdk-python

# Custom endpoint (macOS/Windows)
docker run --rm -e FLOCI_ENDPOINT=http://host.docker.internal:4566 floci-sdk-python
```
