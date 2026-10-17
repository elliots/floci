#!/usr/bin/env python3
"""Qualify lazy native Aurora recovery across real Floci JVM restarts."""
import argparse
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
import uuid

import psycopg


def free_port():
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        return listener.getsockname()[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, default=Path.cwd())
    parser.add_argument('--storage-mode', choices=['persistent', 'hybrid'], default='persistent')
    parser.add_argument('--report', type=Path)
    args = parser.parse_args()
    repo = args.repo.resolve()
    helper = os.environ['FLOCI_TEST_MICROSANDBOX_URL'].rstrip('/')
    token = os.environ['FLOCI_TEST_MICROSANDBOX_TOKEN']
    java = Path(os.environ['JAVA_HOME']) / 'bin/java'
    jar = repo / 'target/quarkus-app/quarkus-run.jar'
    if not jar.is_file() or not java.is_file() or not shutil.which('aws'):
        raise RuntimeError('Build Floci first, then supply Java 25, AWS CLI and the native helper')
    namespace = 'aurora-restart-' + uuid.uuid4().hex[:12]
    root = Path(tempfile.mkdtemp(prefix=namespace + '-'))
    port = free_port()
    endpoint = 'http://127.0.0.1:' + str(port)
    env = dict(os.environ, AWS_ACCESS_KEY_ID='test', AWS_SECRET_ACCESS_KEY='test', AWS_DEFAULT_REGION='us-east-1',
               AWS_EC2_METADATA_DISABLED='true', AWS_PAGER='', AWS_MAX_ATTEMPTS='1')
    env.pop('AWS_SESSION_TOKEN', None)
    env.pop('AWS_PROFILE', None)
    settings = {
        'quarkus.http.port': port, 'floci.port': port, 'floci.base-url': endpoint,
        'floci.storage.mode': args.storage_mode, 'floci.storage.persistent-path': str(root / 'data'),
        'floci.storage.host-persistent-path': str(root / 'data'), 'floci.storage.prune-volumes-on-delete': 'true',
        'floci.services.rds.runtime': 'microsandbox', 'floci.services.rds.mock': 'false',
        'floci.services.rds.microsandbox-url': helper, 'floci.services.rds.microsandbox-token': token,
        'floci.services.rds.microsandbox-root-disk-mib': 6144,
        'floci.services.rds.default-postgres-image': os.environ.get('FLOCI_TEST_POSTGRES_IMAGE', 'postgres:17.11-alpine'),
        'floci.services.rds.endpoint-host': '127.0.0.1', 'floci.services.rds.aurora-resume-delay-millis': 0,
        'floci.docker.resource-namespace': namespace,
        'floci.services.ecs.task-role-credentials.port': free_port(),
        'floci.services.ec2.imds-port': free_port(), 'floci.services.iot.mqtt.enabled': 'false',
    }
    process = None
    logs = []
    cluster_attempted = False
    member_attempted = False
    report = {'storageMode': args.storage_mode, 'artifacts': str(root), 'checks': [], 'passed': False}

    def aws(action, body=None, missing_code=None):
        command = ['aws', '--endpoint-url', endpoint, '--cli-read-timeout', '240', 'rds', action, '--output', 'json']
        if body is not None:
            command += ['--cli-input-json', json.dumps(body)]
        result = subprocess.run(command, env=env, capture_output=True, text=True, timeout=270)
        if result.returncode:
            if missing_code and ('An error occurred (' + missing_code + ')') in result.stderr:
                return {}
            raise RuntimeError('RDS ' + action + ': ' + result.stderr)
        return json.loads(result.stdout) if result.stdout.strip() else {}

    def host(arguments):
        payload = json.dumps({'arguments': arguments, 'input': ''}).encode()
        request = urllib.request.Request(helper + '/v1/command', data=payload,
                                         headers={'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'})
        with urllib.request.urlopen(request, timeout=240) as response:
            result = json.load(response)
        if result['exitCode']:
            raise RuntimeError(str(arguments[:3]) + ': ' + result['stderr'])
        return json.loads(result['stdout'])

    def start():
        nonlocal process
        log = open(root / ('jvm-' + str(len(logs)) + '.log'), 'w')
        logs.append(log)
        process = subprocess.Popen([str(java)] + ['-D' + key + '=' + str(value) for key, value in settings.items()]
                                   + ['-jar', str(jar)], cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError('Floci exited; inspect ' + log.name)
            try:
                aws('describe-db-clusters')
                return
            except RuntimeError:
                time.sleep(1)
        raise RuntimeError('Floci startup timeout; inspect ' + log.name)

    def stop():
        nonlocal process
        if process is not None:
            owned = process
            try:
                owned.terminate()
                try:
                    owned.wait(timeout=120)
                except subprocess.TimeoutExpired:
                    owned.kill()
                    owned.wait(timeout=30)
                    raise RuntimeError('Floci did not stop within its shutdown deadline')
                if owned.returncode not in (0, 143):
                    raise RuntimeError('Floci shutdown exit ' + str(owned.returncode))
            finally:
                process = None

    def machine():
        matches = []
        for path in (root / 'data').rglob('rds-microsandbox.json'):
            matches.extend(value for value in json.loads(path.read_text()).values()
                           if value.get('instanceId') == namespace)
        if len(matches) != 1:
            raise AssertionError('Expected exactly one owned runtime journal, found ' + str(len(matches)))
        return matches[0]

    def guest():
        return host(['inspect', '--format', 'json', machine()['vm']])

    def cluster():
        return aws('describe-db-clusters', {'DBClusterIdentifier': namespace})['DBClusters'][0]

    def sql(query):
        with psycopg.connect(host='127.0.0.1', port=database_port, user='master', password='Restart123!',
                             dbname='appdb', sslmode='disable', connect_timeout=120, autocommit=True) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall() if cursor.description else []

    def checked(name):
        report['checks'].append(name)
        print(name, flush=True)

    try:
        if host(['ls', '--format', 'json']) or host(['snap', 'ls', '--format', 'json']):
            raise RuntimeError('Use an empty dedicated native runtime home for this fixture')
        start()
        cluster_attempted = True
        result = aws('create-db-cluster', {'DBClusterIdentifier': namespace, 'Engine': 'aurora-postgresql',
                    'MasterUsername': 'master', 'MasterUserPassword': 'Restart123!', 'DatabaseName': 'appdb',
                    'ServerlessV2ScalingConfiguration': {'MinCapacity': 0, 'MaxCapacity': 2, 'SecondsUntilAutoPause': 300}})
        member_attempted = True
        aws('create-db-instance', {'DBInstanceIdentifier': namespace + '-writer',
            'DBClusterIdentifier': namespace, 'Engine': 'aurora-postgresql', 'DBInstanceClass': 'db.serverless'})
        database_port = result['DBCluster']['Port']
        original_endpoint = result['DBCluster']['Endpoint']
        sql("CREATE TABLE restart_marker(v text); INSERT INTO restart_marker VALUES ('durable')")
        original_boot = sql('SELECT pg_postmaster_start_time()')[0][0]
        assert machine()['rootDiskMib'] == 6144
        assert guest()['config']['image']['Oci']['root_disk']['size_mib'] == 6144
        assert guest()['storage']['logical_bytes'] >= 6144 * 1024 * 1024
        original_guest_id = machine()['guestIds'][machine()['vm']]
        stop()
        assert machine()['phase'] == 'STOPPED'
        assert guest()['status'] == 'Stopped'
        start()
        description = cluster()
        assert (description['Endpoint'], description['Port']) == (original_endpoint, database_port)
        assert description['Status'] == 'available'
        time.sleep(3)
        assert guest()['status'] == 'Stopped', 'Metadata requests must not wake the recovered DB'
        assert machine()['guestIds'][machine()['vm']] == original_guest_id
        checked('JVM restart leaves the owned cold database stopped with its original endpoint')
        wake_started = time.monotonic()
        assert sql('SELECT v FROM restart_marker') == [('durable',)]
        report['coldWakeSeconds'] = round(time.monotonic() - wake_started, 3)
        cold_boot = sql('SELECT pg_postmaster_start_time()')[0][0]
        assert cold_boot != original_boot
        checked('First SQL request cold-starts the original database and reads its durable row')
        deadline = time.monotonic() + 420
        while time.monotonic() < deadline:
            if machine()['phase'] == 'HIBERNATED' and guest()['status'] == 'Stopped':
                break
            time.sleep(2)
        else:
            raise AssertionError('Aurora did not auto-pause after the AWS minimum 300 idle seconds')
        checkpoint = machine()['snapshot']
        checkpoint_id = machine()['checkpointIds'][checkpoint]
        stop()
        assert machine()['phase'] == 'HIBERNATED'
        assert machine()['snapshot'] == checkpoint
        assert guest()['status'] == 'Stopped'
        start()
        description = cluster()
        assert (description['Endpoint'], description['Port']) == (original_endpoint, database_port)
        time.sleep(3)
        assert machine()['phase'] == 'HIBERNATED' and guest()['status'] == 'Stopped'
        assert machine()['checkpointIds'][checkpoint] == checkpoint_id
        checked('Shutdown and metadata recovery preserve the stopped guest and immutable RAM checkpoint')
        wake_started = time.monotonic()
        assert sql('SELECT v FROM restart_marker') == [('durable',)]
        report['ramWakeSeconds'] = round(time.monotonic() - wake_started, 3)
        assert sql('SELECT pg_postmaster_start_time()')[0][0] == cold_boot
        assert machine()['generation'] == 1
        checked('First SQL request restores RAM after JVM restart without restarting PostgreSQL')
        report['passed'] = True
    finally:
        original_failure = sys.exc_info()[0] is not None
        failures = []
        if cluster_attempted:
            try:
                if process is None or process.poll() is not None:
                    process = None
                    start()
            except Exception as failure:
                failures.append('Cleanup startup: ' + str(failure))
            if process is not None and process.poll() is None:
                deletions = []
                if member_attempted:
                    deletions.append(('delete-db-instance', {'DBInstanceIdentifier': namespace + '-writer',
                                      'SkipFinalSnapshot': True}, 'DBInstanceNotFound'))
                deletions.append(('delete-db-cluster', {'DBClusterIdentifier': namespace,
                                  'SkipFinalSnapshot': True}, 'DBClusterNotFoundFault'))
                for action, body, missing in deletions:
                    try:
                        aws(action, body, missing_code=missing)
                    except Exception as failure:
                        failures.append(action + ': ' + str(failure))
        try:
            stop()
        except Exception as failure:
            failures.append('JVM shutdown: ' + str(failure))
        try:
            remaining = host(['ls', '--format', 'json'])
            checkpoints = host(['snap', 'ls', '--format', 'json'])
            report['cleanup'] = {'guests': remaining, 'checkpoints': checkpoints}
            if remaining or checkpoints:
                failures.append('Owned native cleanup is incomplete')
        except Exception as failure:
            failures.append('Native inventory: ' + str(failure))
        for log in logs:
            log.close()
        if failures:
            report['cleanupErrors'] = failures
            report['passed'] = False
        if args.report:
            args.report.write_text(json.dumps(report, indent=2) + '\n')
        print('Artifacts: ' + str(root), flush=True)
        if failures:
            if not original_failure:
                raise RuntimeError('; '.join(failures))
            print('Cleanup failures: ' + '; '.join(failures), file=sys.stderr)



if __name__ == '__main__':
    main()
