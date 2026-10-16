# Shared runtime integration tests

The RDS tests tagged `runtime` run the same AWS requests and workload assertions
against Docker and microsandbox. `FLOCI_TEST_RUNTIME` selects the fixture backend;
the AWS and workload assertions do not change with that setting. Docker is the default for the
ordinary Maven suite. An explicitly selected backend must be available: the matrix
fails instead of silently skipping a missing Docker daemon or native helper.

## Keep one copy of each contract

Existing Query and IAM suites keep their names, AWS requests and workload assertions.
`RuntimeTestProfile` selects the database backend, while `RdsRuntimeTestSupport`
contains availability checks, cleanup and the few physical-runtime inspections
that differ. Baseline EC2 tests are outside this matrix; new EC2 fixtures are
parked in `parked/microsandbox-ec2-20261017`.
SQL assertions connect through the database endpoint. Do not copy a test class
or add a Docker/native branch inside an AWS behavior assertion.

New methods in an existing shared class run in both matrix entries automatically.
A new shared class needs `@Tag("runtime")` and the appropriate profile and fixture;
the runner discovers it without another class list. Existing Docker-only classes
are not automatically converted. A backend-specific regression belongs beside
that backend's implementation tests. The runner launches the same shared classes
in separate Maven processes; it does not maintain a second set of test scenarios.

The initial extraction removes duplicated Docker setup and guest-exec helpers.
Subsequent backend changes should stay in the fixture or profile. This keeps
rebases focused on the original behavior tests and avoids parallel test hierarchies.

## Run the matrix

Run both backends sequentially with Docker, Java 25 and the native helper running:

```sh
export FLOCI_TEST_MICROSANDBOX_URL=http://127.0.0.1:8186
export FLOCI_TEST_MICROSANDBOX_TOKEN=your-local-runtime-token
bash tools/runtime-tests/run.sh both
```

Use `docker` or `microsandbox` to run one side. Extra arguments go to Maven, for
example `bash tools/runtime-tests/run.sh docker -Dtest=RdsMysqlRuntimeIntegrationTest`.
For one suite the equivalent direct invocation is:

```sh
FLOCI_TEST_RUNTIME=microsandbox ./mvnw test -Dtest=RdsMysqlRuntimeIntegrationTest -Dgroups=runtime
```

The RDS contracts cover:

| Suite | Shared behavior |
| --- | --- |
| `RdsPostgresRuntimeIntegrationTest` | SDK create/delete, active connection prevents idle pause, repeated pause/wake retains process memory before and after ordinary stop/start, rows and rotated credentials survive snapshot/restore, omitted DBName retains the default database |
| `RdsMysqlRuntimeIntegrationTest` | SDK Aurora pause/wake, InnoDB and MEMORY tables, master privileges, root and named master password rotation, ordinary stop/start persistence |
| `RdsStopStartIntegrationTest` | Query XML lifecycle statuses and errors, stable endpoint, refused connections while stopped, cluster members and reboot |
| `RdsSnapshotLifecycleIntegrationTest` | Query snapshot copy/tags/delete/errors and data restored from the copy |
| `RdsClusterSnapshotLifecycleIntegrationTest` | Cluster snapshot copy, sharing attributes, deletion and data restored from the copy |
| `RdsPointInTimeRestoreIntegrationTest` | Instance/cluster point-in-time restore, resource identifiers, renamed databases, restorable window and refusal errors |
| `RdsAuroraAutoPauseIntegrationTest` | Available status while paused, zero capacity metrics, pause/resume events and account ownership |
| `RdsDataResumingIntegrationTest` | Data API `DatabaseResumingException` and successful retries |
| `RdsPostgresStartupParametersIntegrationTest` | Password/IAM startup parameters, session authorization, role defaults and privilege escalation refusal |
| `RdsMemberIamResourceIntegrationTest` | IAM cluster policy on member endpoints before and after reboot |
| `RdsIamAuthLambdaSessionIntegrationTest` | Signed temporary credentials, IAM auth toggles, role sessions, password changes and authentication refusal |

PostgreSQL defaults to `postgres:17.11-alpine` and MySQL to `mysql:8.0.45` in both
fixtures. `FLOCI_TEST_POSTGRES_IMAGE` and `FLOCI_TEST_MYSQL_IMAGE` override those
images for both sides. Pull the Docker images before a timed test run on a fresh
machine (`docker pull postgres:17.11-alpine` and `docker pull mysql:8.0.45`);
registry downloads can otherwise consume the test deadline. These suites create real databases and use endpoint JDBC
connections, including the legacy Query tests that previously executed SQL through
Docker. Their idle-clock fixture calls the selected provider's idle check so the
tests do not wait the AWS minimum of five minutes for every pause.
Physical runtime inspections additionally verify that pause releases or freezes
execution and resume actually restarts it. Each test tracks its newly created
databases and attempts SDK cleanup even after an assertion fails. Resource locks
serialize these shared RDS tests, and each JVM gets its own Docker resource namespace.

The old `MicrosandboxRdsIntegrationTest` and `MicrosandboxMysqlIntegrationTest`
names are replaced by the shared PostgreSQL and MySQL suites above. Their shared
snapshot coverage also replaces `RdsContainerManagerPostgresTest` and the
PostgreSQL case in `RdsAwsIntegrationTest`; the latter retains its SQL Server case.


## Remaining coverage boundaries

This matrix is not a claim of complete feature parity. SQL Server still needs a
native backend; its existing Docker test remains. Backend unit tests for ownership,
cleanup, command construction and recovery remain valuable and are not replaced by
the shared AWS contracts. Docker-specific container and volume mechanics tests also
remain. Existing management-only AWS SDK tests already exercise the shared service
layer once; they do not need a duplicate backend matrix when no workload is launched.

EKS management APIs already use the same service implementation. Native pods are
opt-in workers in a Kubernetes cluster whose control plane still runs on Docker.
Existing EKS management and Docker control-plane tests remain applicable; passing
them alone does not establish native pod compatibility with sidecars, projected
credentials, persistent volumes or every Kubernetes networking feature. Those gaps
must retain their assertions as features gain native support, rather than being
skipped or given weaker expectations in this matrix.

Native pods also do not yet consume EKS node-group registry-host configuration or
the existing Docker `EmbeddedDns` injection. EKS control-plane log tests run once
against that shared Docker control plane. Lambda and ElastiCache have no native
backend, so their existing Docker runtime suites remain unchanged.
Native pod execution is covered separately by
[`tools/microsandbox-pods/qualify.py`](../microsandbox-pods/qualify.py).

See the dated [validation record](../microsandbox/VALIDATION.md) for the exercised
platforms, results and limits of the SDK migration checks.
