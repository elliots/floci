# On-demand runtime rebase: project migration notes

The branch was rebuilt from `7809fd1f5` onto upstream `main` at `340e727ed`
(Floci 2.3.0, Quarkus 3.40.1). The original history is preserved locally as
`backup/on-demand-runtime-before-main-20261015`. Nothing was pushed.

## Features retained

- On-demand EC2, ECS, and EKS activation, HTTP/WebSocket/gRPC gateway forwarding,
  queue triggers, readiness checks, idle shutdown, and ECS task protection.
- Workload network isolation, local origin routing, wildcard origins, and local TLS.
- ElastiCache tags, paginated user-group listing, Redis TLS, and the
  `floci:tls-hostnames` tag.
- EC2 `CreateDefaultSubnet` and the SDK/Terraform compatibility fixtures.
- CloudFront viewer-host lookup across accounts.
- Aurora MySQL driver metadata emulation, RDS cluster KMS/parameter-group response
  fields, correctly named `DBClusterMember` XML elements, and the existing local
  RDS TLS inspection helpers.

## Changes your project may need

### Aurora version and instance-class lookup

The old branch treated any syntactically plausible Aurora version/class pair as
orderable. That shortcut was dropped because upstream now has an engine catalogue.
`DescribeOrderableDBInstanceOptions` only returns combinations in that catalogue.
`StorageType=aurora` is still emitted for its Aurora rows, so Terraform's
`storage_type = "aurora"` filter continues to work.

At this base, the catalogue's orderable Aurora versions are:

| Engine | Version | Instance classes |
|---|---|---|
| `aurora-mysql` | `8.0.mysql_aurora.3.05.2` | `db.t3.medium`, `db.t4g.medium`, `db.r5.large`, `db.r6g.large` |
| `aurora-postgresql` | `16.3` | `db.t3.medium`, `db.t4g.medium`, `db.r5.large`, `db.r6g.large` |

Update local Terraform data-source filters to an advertised combination. This is
Floci's finite local catalogue, not a statement that AWS only offers these options.
Other advertised engine versions do not necessarily have orderable classes.

### RDS KMS keys

The old branch echoed an arbitrary `KmsKeyId` without resolving it. Explicit keys
now use the same lookup as upstream RDS instances: create the key in local KMS,
set `StorageEncrypted=true`, and pass its ID, ARN, or alias. RDS reports the
resolved key ARN. A missing, disabled, or pending-deletion key produces
`KMSKeyNotAccessibleFault` before a cluster is created. An explicit key without
storage encryption is rejected.

Replace production-only/fake KMS identifiers in local configuration with a locally
created key. Compare the returned ARN, rather than expecting an alias or bare ID
to be echoed. This follows the identifier forms in the
[AWS CreateDBCluster contract](https://docs.aws.amazon.com/AmazonRDS/latest/APIReference/API_CreateDBCluster.html).
The emulator still does not encrypt its container's disk with that key.

### ElastiCache user groups

Upstream's implementation replaces the branch's duplicate lifecycle implementation:

- Creation returns `active` immediately. Use the SDK waiter or accept `active`;
  do not require observing `creating` first.
- Replication groups retain actual user-group associations. Changes to membership
  affect authentication dynamically, instead of expanding a group into a fixed
  list of user IDs at association time.
- `UserGroupIdsToAdd` and `UserGroupIdsToRemove` should contain user-group IDs.
  Do not depend on the old branch's expansion helper or pass user IDs as groups.
- Groups attached to replication groups or serverless caches must be detached
  before deletion. Update teardown order if necessary.
- Tags and pagination from this branch are retained. Tag operations also preserve
  upstream's serverless-cache support and account/partition checks.

Persisted user-group keys differ: the old branch used
`<account>/<region>::<user-group-id>`, while upstream uses
`<account>/<user-group-id>`. There is no automatic conversion in this rebase.
For a local environment created with the old branch, use a fresh data directory
and reapply the infrastructure, keeping the old directory intact until verified.
Do not reuse the old user-group file unchanged or edit JSON without accounting for
its WAL. Within one account, use distinct group IDs across regions in this emulator.

### ElastiCache TLS configuration

`FLOCI_SERVICES_ELASTICACHE_TLS_SUFFIXES` keeps the same name. Its default is now
`auto`, which derives cache domains from all known AWS partitions. Commercial
cache endpoints keep working; China and other partitions are also covered.
An explicitly configured comma-separated domain list replaces the automatic list.

`floci:tls-hostnames` still supplies names for clients that do not send SNI.
Trust Floci's CA and keep hostname verification enabled. This local proxy continues
to accept plaintext and TLS on the same port; it does not enforce AWS's
TLS-only mode. TLS support here is a local connectivity feature.

### Runtime, networking, and CloudFront

Existing `FLOCI_ON_DEMAND_*`, `FLOCI_NETWORK_CONFIG_FILE`, workload YAML, and origin
route configuration keep their names and shapes. Wildcard origins remain supported.
Upstream container initialization, network-namespace handling, account-aware cache
restoration, and IAM validation against published RDS ports are retained.

CloudFront retains upstream range serving, streaming, and disconnect handling.
The branch's cross-account viewer-host resolution remains in place; duplicate
alternate domain names across accounts remain rejected.

### Local RDS TLS helpers and Aurora emulation

The branch's existing `GET /_aws/rds/tls/ca` and `POST /_aws/rds/tls/hosts` helpers
are retained as local tooling endpoints, not AWS management APIs. The host POST
still accepts `{"hosts":["database.example.test"]}` and returns 204.

Aurora MySQL metadata emulation still presents the local writer as a single-node
topology for the driver's supported probe queries. It does not implement AWS
Aurora replication or failover. Actual SQL continues to run on the local backend.

## Upstream work used instead of duplicate branch code

- ElastiCache user groups, replication-group associations, modeled user errors,
  and restoration of resources belonging to non-default accounts.
- RDS Aurora catalogue support, cluster-owned credentials and IAM settings,
  and validation of IAM tokens against the endpoint's published port.
- Upstream generated release history. The branch's hand-written changelog entries
  were omitted; the focused conventional commits describe its retained changes.

## Validation

Validation used Java 25 and the rebuilt JVM application:

- `./mvnw package -DskipTests` completed successfully.
- Focused unit and integration suites passed: 2,812 tests across 112 suites,
  counting each suite's final run after fixes. Coverage includes on-demand
  activation, gateways, queue triggers, network isolation, Docker lifecycle,
  CloudFront, EC2, ECS, EKS, ElastiCache, RDS, and partition inventory.
- Real Docker tests passed for EC2, ECS, EKS, workload network isolation, and RDS
  IAM authentication. The RDS IAM test passed with the repository's pinned
  `postgres:16.3-alpine` after its initial image download completed.
- The separate AWS SDK compatibility module passed `Ec2DefaultSubnetTest` and
  `ElastiCacheUserGroupTagsTest` against the packaged application.
- Checkstyle, generated-documentation checks, the partition-literal gate, and
  `git diff --check` passed.

Native-image and the full repository-wide compatibility matrix were not run.
