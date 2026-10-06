"""Opt-in on-demand compatibility tests using real Kubernetes pods, ECS tasks, HTTP, and gRPC."""

import concurrent.futures
import http.client
import json
import os
from pathlib import Path
import queue
import subprocess
import time
from urllib.parse import urlsplit

import boto3
import pytest


def wait_for(predicate, seconds=60):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.25)
    raise AssertionError("Timed out waiting for runtime state")


@pytest.fixture(scope="module")
def gateway():
    endpoint = os.environ.get("FLOCI_ON_DEMAND_TEST_GATEWAY")
    if not endpoint:
        pytest.skip("Set FLOCI_ON_DEMAND_TEST_GATEWAY to test a configured on-demand process")
    return endpoint


@pytest.fixture(scope="module")
def kubernetes_runtime(aws_config, gateway):
    endpoint = gateway
    cluster = os.environ.get("FLOCI_ON_DEMAND_TEST_CLUSTER", "on-demand-live")
    eks = boto3.client("eks", **aws_config)
    sqs = boto3.client("sqs", **aws_config)
    queue_url = None
    created = False
    try:
        eks.create_cluster(name=cluster, roleArn="arn:aws:iam::000000000000:role/on-demand-test",
                           resourcesVpcConfig={"subnetIds": []})
        created = True
        wait_for(lambda: eks.describe_cluster(name=cluster)["cluster"]["status"] == "ACTIVE", 180)
        containers = subprocess.run(
            ["docker", "ps", "-q", "--filter", "label=io.floci.service=eks",
             "--filter", f"label=io.floci.resource-id={cluster}",
             "--filter", "label=io.floci.account=000000000000"],
            check=True, capture_output=True, text=True, timeout=10).stdout.split()
        assert len(containers) == 1, "Use a unique cluster name with exactly one test owner"
        container = containers[0]
        manifest = Path(__file__).with_name("fixtures").joinpath("on-demand-runtime.yaml").read_text()
        subprocess.run(["docker", "exec", "-i", container, "kubectl", "apply", "-f", "-"],
                       input=manifest, text=True, capture_output=True, check=True, timeout=30)
        queue_url = sqs.create_queue(QueueName="on-demand-live-events")["QueueUrl"]
        yield KubernetesRuntime(endpoint, container, sqs, queue_url)
    finally:
        if queue_url:
            sqs.delete_queue(QueueUrl=queue_url)
        if created:
            eks.delete_cluster(name=cluster)
        sqs.close()
        eks.close()


class KubernetesRuntime:
    def __init__(self, endpoint, container, sqs, queue_url):
        self.endpoint = urlsplit(endpoint)
        self.container = container
        self.sqs = sqs
        self.queue_url = queue_url

    def request(self, path, method="GET", body=None):
        connection = http.client.HTTPConnection(self.endpoint.hostname, self.endpoint.port, timeout=60)
        try:
            connection.request(method, path, body=body,
                               headers={"Host": "on-demand.dev.test", "Authorization": "Bearer integration-test"})
            response = connection.getresponse()
            return response.status, {name.lower(): value for name, value in response.getheaders()}, response.read()
        finally:
            connection.close()

    def deployment(self):
        result = subprocess.run(
            ["docker", "exec", self.container, "kubectl", "get", "deployment", "on-demand-app", "-o", "json"],
            check=True, capture_output=True, text=True, timeout=10)
        return json.loads(result.stdout)

    def sleeping(self):
        deployment = self.deployment()
        return deployment["spec"]["replicas"] == 0 and deployment["status"].get("replicas", 0) == 0

    def ready(self):
        deployment = self.deployment()
        return (deployment["spec"]["replicas"] == 1
                and deployment["status"].get("observedGeneration", 0) >= deployment["metadata"]["generation"]
                and deployment["status"].get("availableReplicas", 0) == 1)


@pytest.mark.timeout(240)
def test_sleeping_health_does_not_start_a_pod(kubernetes_runtime):
    wait_for(kubernetes_runtime.sleeping)
    wait_for(lambda: kubernetes_runtime.request("/health")[0] == 200)
    for _ in range(8):
        status, headers, body = kubernetes_runtime.request("/health?probe=true")
        assert status == 200
        assert headers["cache-control"] == "no-store"
        assert body == b'{"status":"UP"}'
        assert kubernetes_runtime.sleeping()
        time.sleep(0.5)


@pytest.mark.timeout(240)
def test_concurrent_cold_http_requests_preserve_binary_body_and_headers(kubernetes_runtime):
    wait_for(kubernetes_runtime.sleeping)
    body = bytes(range(256)) * 32
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
        futures = [executor.submit(kubernetes_runtime.request, "/echo?a=%2F&b=two+words", "POST", body) for _ in range(12)]
        responses = [future.result() for future in futures]
    assert kubernetes_runtime.ready()
    pod_ids = set()
    for status, headers, returned in responses:
        assert status == 200
        assert returned == body
        assert headers["x-request-path"] == "/echo?a=%2F&b=two+words"
        assert headers["x-request-host"] == "on-demand.dev.test"
        assert headers["x-request-auth"] == "Bearer integration-test"
        pod_ids.add(headers["x-pod-name"])
    assert len(pod_ids) == 1
    assert kubernetes_runtime.request("/health")[0] == 503, "Running application health failures must remain visible"
    wait_for(kubernetes_runtime.sleeping)


@pytest.mark.timeout(240)
def test_an_active_http_request_outlives_the_idle_timeout(kubernetes_runtime):
    wait_for(kubernetes_runtime.sleeping)
    assert kubernetes_runtime.request("/ready")[0] == 200
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        pending = executor.submit(kubernetes_runtime.request, "/slow")
        wait_for(kubernetes_runtime.ready)
        time.sleep(4)
        assert kubernetes_runtime.ready(), "The three-second idle timeout must not stop an active request"
        assert pending.result()[0] == 200
    wait_for(kubernetes_runtime.sleeping)


@pytest.mark.timeout(240)
def test_delayed_and_inflight_sqs_messages_hold_the_real_deployment(kubernetes_runtime):
    wait_for(kubernetes_runtime.sleeping)
    kubernetes_runtime.sqs.send_message(QueueUrl=kubernetes_runtime.queue_url, MessageBody="integration-event", DelaySeconds=2)
    time.sleep(0.5)
    assert kubernetes_runtime.sleeping(), "A delayed message should activate only when it becomes visible"
    wait_for(kubernetes_runtime.ready)
    attributes = kubernetes_runtime.sqs.get_queue_attributes(QueueUrl=kubernetes_runtime.queue_url,
                                                 AttributeNames=["ApproximateNumberOfMessages"])["Attributes"]
    assert attributes["ApproximateNumberOfMessages"] == "1", "The scaler must not consume the message"
    messages = kubernetes_runtime.sqs.receive_message(QueueUrl=kubernetes_runtime.queue_url, VisibilityTimeout=30)["Messages"]
    assert len(messages) == 1
    assert messages[0]["Body"] == "integration-event"
    time.sleep(4)
    assert kubernetes_runtime.ready(), "In-flight work must prevent idle shutdown"
    kubernetes_runtime.sqs.delete_message(QueueUrl=kubernetes_runtime.queue_url, ReceiptHandle=messages[0]["ReceiptHandle"])
    wait_for(kubernetes_runtime.sleeping)
    assert subprocess.run(["docker", "inspect", "-f", "{{.State.Running}}", kubernetes_runtime.container],
                          check=True, capture_output=True, text=True, timeout=10).stdout.strip() == "true"


@pytest.fixture(scope='module')
def ecs_runtime(aws_config, gateway):
    endpoint = gateway
    grpc = pytest.importorskip('grpc', reason='Install requirements-on-demand.txt')
    image = os.environ.get('FLOCI_ON_DEMAND_TEST_ECS_IMAGE', 'floci-on-demand-grpc-test:local')
    fixture = Path(__file__).with_name('fixtures') / 'on-demand-ecs-grpc'
    subprocess.run(['docker', 'build', '-t', image, str(fixture)],
                   check=True, capture_output=True, text=True, timeout=240)
    ecs = boto3.client('ecs', **aws_config)
    sqs = boto3.client('sqs', **aws_config)
    cluster = os.environ.get('FLOCI_ON_DEMAND_TEST_ECS_CLUSTER', 'on-demand-ecs-live')
    port = int(os.environ.get('FLOCI_ON_DEMAND_TEST_ECS_BACKEND_PORT', '15051'))
    task_definition = None
    service_created = False
    queue_url = None
    ecs.create_cluster(clusterName=cluster)
    try:
        health = "import grpc; c=grpc.insecure_channel('localhost:50051'); " \
                 "assert c.unary_unary('/test.Echo/Unary')(b'ready',timeout=1)==b'ready'"
        task_definition = ecs.register_task_definition(
            family=cluster, networkMode='bridge',
            containerDefinitions=[{'name': 'grpc', 'image': image, 'essential': True,
                                   'portMappings': [{'containerPort': 50051, 'hostPort': port}],
                                   'healthCheck': {'command': ['CMD', 'python', '-c', health],
                                                   'interval': 5, 'timeout': 2, 'retries': 3}}])['taskDefinition']
        ecs.create_service(cluster=cluster, serviceName='grpc',
                           taskDefinition=task_definition['taskDefinitionArn'],
                           desiredCount=0, launchType='EC2')
        service_created = True
        queue_url = sqs.create_queue(QueueName='on-demand-ecs-live-events')['QueueUrl']
        address = urlsplit(endpoint)
        with grpc.insecure_channel(f'{address.hostname}:{address.port}',
                                   options=(('grpc.default_authority', 'ecs-on-demand.dev.test'),)) as channel:
            yield EcsGrpcRuntime(ecs, sqs, cluster, queue_url, channel, grpc)
    finally:
        if queue_url:
            sqs.delete_queue(QueueUrl=queue_url)
        if service_created:
            task_arns = ecs.list_tasks(cluster=cluster, serviceName='grpc')['taskArns']
            ecs.delete_service(cluster=cluster, service='grpc', force=True)
            for task_arn in task_arns:
                ecs.stop_task(cluster=cluster, task=task_arn, reason='on-demand compatibility test cleanup')
            if task_arns:
                wait_for(lambda: all(task['lastStatus'] == 'STOPPED' for task in
                                     ecs.describe_tasks(cluster=cluster, tasks=task_arns)['tasks']))
        if task_definition:
            ecs.deregister_task_definition(taskDefinition=task_definition['taskDefinitionArn'])
        ecs.delete_cluster(cluster=cluster)
        ecs.close()
        sqs.close()


class EcsGrpcRuntime:
    metadata = (('authorization', 'Bearer integration-test'), ('customer-bin', b'\x00\xff'))

    def __init__(self, ecs, sqs, cluster, queue_url, channel, grpc):
        self.ecs = ecs
        self.sqs = sqs
        self.cluster = cluster
        self.queue_url = queue_url
        self.channel = channel
        self.grpc = grpc

    def tasks(self, status='RUNNING'):
        arns = self.ecs.list_tasks(cluster=self.cluster, serviceName='grpc', desiredStatus=status)['taskArns']
        return self.ecs.describe_tasks(cluster=self.cluster, tasks=arns)['tasks'] if arns else []

    def sleeping(self):
        service = self.ecs.describe_services(cluster=self.cluster, services=['grpc'])['services'][0]
        return (service['desiredCount'] == 0 and not self.tasks()
                and all(task['lastStatus'] == 'STOPPED' for task in self.tasks('STOPPED')))

    def unary(self, data, timeout=90):
        return self.channel.unary_unary('/test.Echo/Unary')(data, timeout=timeout, metadata=self.metadata)


@pytest.mark.timeout(240)
def test_concurrent_cold_grpc_calls_preserve_bytes_metadata_and_trailers(ecs_runtime):
    wait_for(ecs_runtime.sleeping)
    payload = bytes(range(256)) * 4096
    rpc = ecs_runtime.channel.unary_unary('/test.Echo/Unary')
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
        calls = [executor.submit(rpc.with_call, payload, timeout=90, metadata=ecs_runtime.metadata,
                                 compression=ecs_runtime.grpc.Compression.Gzip) for _ in range(12)]
        instances = set()
        for future in calls:
            echoed, call = future.result()
            assert echoed == payload
            initial = dict(call.initial_metadata())
            trailers = dict(call.trailing_metadata())
            assert initial['initial-bin'] == b'\x00\xff'
            assert trailers['grpc-status-details-bin'] == b'\x01\x02'
            assert trailers['result'] == 'complete'
            instances.add(initial['x-instance'])
    assert len(instances) == 1
    task = ecs_runtime.tasks()[0]
    inspect = subprocess.run(['docker', 'inspect', task['containers'][0]['runtimeId']],
                             check=True, capture_output=True, text=True, timeout=10)
    container = json.loads(inspect.stdout)[0]
    assert container['State']['Health']['Status'] == 'healthy'
    assert container['Config']['Healthcheck']['Interval'] == 5_000_000_000
    assert container['Config']['Healthcheck']['Timeout'] == 2_000_000_000
    assert container['Config']['Healthcheck']['Retries'] == 3
    assert container['Config']['Healthcheck']['Test'][0] == 'CMD'
    with pytest.raises(ecs_runtime.grpc.RpcError) as error:
        ecs_runtime.unary(b'error')
    assert error.value.code() == ecs_runtime.grpc.StatusCode.PERMISSION_DENIED
    assert error.value.details() == 'denied here'


@pytest.mark.timeout(240)
def test_all_grpc_streaming_modes_and_cancellation_hold_then_release_tasks(ecs_runtime):
    wait_for(ecs_runtime.sleeping)
    server = ecs_runtime.channel.unary_stream('/test.Echo/ServerStream')
    responses = list(server(b'message', timeout=90, metadata=ecs_runtime.metadata))
    assert responses == [b'message' + bytes([i]) for i in range(6)]
    assert len(ecs_runtime.tasks()) == 1, 'six-second stream must outlive the three-second idle interval'
    client = ecs_runtime.channel.stream_unary('/test.Echo/ClientStream')
    assert client(iter([b'one', b'two']), timeout=90, metadata=ecs_runtime.metadata) == b'onetwo'
    messages = queue.Queue()

    def requests():
        while True:
            message = messages.get(timeout=30)
            if message is None:
                return
            yield message

    bidi = ecs_runtime.channel.stream_stream('/test.Echo/Bidi')(requests(), timeout=90, metadata=ecs_runtime.metadata)
    messages.put(b'first')
    assert next(bidi) == b'first', 'response must arrive before the request stream is half-closed'
    time.sleep(5)
    assert len(ecs_runtime.tasks()) == 1
    messages.put(b'last')
    assert next(bidi) == b'last'
    assert bidi.cancel()
    messages.put(None)
    assert ecs_runtime.unary(b'sibling') == b'sibling', 'cancellation must leave the shared HTTP/2 connection usable'
    wait_for(ecs_runtime.sleeping)


@pytest.mark.timeout(240)
def test_ecs_task_protection_holds_background_work_after_the_rpc_finishes(ecs_runtime):
    wait_for(ecs_runtime.sleeping)
    assert ecs_runtime.unary(b'work') == b'work'
    task = ecs_runtime.tasks()[0]
    protection = ecs_runtime.ecs.update_task_protection(cluster=ecs_runtime.cluster, tasks=[task['taskArn']],
                                                    protectionEnabled=True)['protectedTasks'][0]
    assert protection['protectionEnabled']
    assert protection['expirationDate'].timestamp() > time.time() + 7100
    time.sleep(6)
    assert ecs_runtime.tasks()[0]['taskArn'] == task['taskArn']
    ecs_runtime.ecs.update_task_protection(cluster=ecs_runtime.cluster, tasks=[task['taskArn']], protectionEnabled=False)
    wait_for(ecs_runtime.sleeping)
    assert ecs_runtime.unary(b'restart') == b'restart'
    assert ecs_runtime.tasks()[0]['taskArn'] != task['taskArn'], 'waking an ECS service creates a new task'


@pytest.mark.timeout(240)
def test_sqs_delays_inflight_holds_and_grpc_deadlines_release_ecs_tasks(ecs_runtime):
    wait_for(ecs_runtime.sleeping)
    ecs_runtime.sqs.send_message(QueueUrl=ecs_runtime.queue_url, MessageBody='customer-event', DelaySeconds=3)
    time.sleep(1)
    assert ecs_runtime.sleeping()
    wait_for(lambda: bool(ecs_runtime.tasks()))
    messages = ecs_runtime.sqs.receive_message(QueueUrl=ecs_runtime.queue_url, VisibilityTimeout=30)['Messages']
    assert len(messages) == 1, 'the scaler must not consume the event'
    time.sleep(5)
    assert ecs_runtime.tasks()
    ecs_runtime.sqs.delete_message(QueueUrl=ecs_runtime.queue_url, ReceiptHandle=messages[0]['ReceiptHandle'])
    wait_for(ecs_runtime.sleeping)
    assert ecs_runtime.unary(b'ready') == b'ready'
    with pytest.raises(ecs_runtime.grpc.RpcError) as error:
        ecs_runtime.unary(b'slow', timeout=0.2)
    assert error.value.code() == ecs_runtime.grpc.StatusCode.DEADLINE_EXCEEDED
    wait_for(ecs_runtime.sleeping)
