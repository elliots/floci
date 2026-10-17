# Aurora zero-capacity engine versions

`CreateDBCluster` and scaling changes through `ModifyDBCluster` now reject
`MinCapacity=0` when the effective engine version cannot support auto-pause.
Validation applies to both Docker and native runtimes through the same RDS service.

The version floors follow the [AWS Aurora auto-pause documentation](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-serverless-v2-auto-pause.html):

| Engine | Minimum version |
| --- | --- |
| Aurora PostgreSQL 13 | 13.15 |
| Aurora PostgreSQL 14 | 14.12 |
| Aurora PostgreSQL 15 | 15.7 |
| Aurora PostgreSQL 16 | 16.3 |
| Later Aurora PostgreSQL major versions | Supported engine releases |
| Aurora MySQL | `8.0.mysql_aurora.3.08.0` |

The Aurora MySQL catalogue default changes from `8.0.mysql_aurora.3.05.2` to
`8.0.mysql_aurora.3.08.0`. New clusters that omit a version use the supported
default. Recognized major aliases, such as PostgreSQL `16` and MySQL `8.0`, now
resolve to a concrete catalogue version before the cluster is recorded.

Existing explicitly stored versions are not changed. An older cluster can remain
at nonzero minimum capacity. Enabling zero capacity, or changing an existing
zero-capacity configuration, requires a supported concrete version. A rejected
modify leaves its credentials and scaling configuration unchanged.

Legacy records without a concrete known version are not silently assigned the
new default. They keep their recorded metadata and data, but cannot auto-pause
or accept a zero-capacity scaling update until their version is established.
Create a new cluster with an explicit supported version and migrate its data
when the old version cannot be established safely. Do not rewrite metadata to
claim an engine upgrade that has not taken place.

Version checks do not add engine binary upgrades, runtime ACU resizing, or full
modeling of replication, zero-ETL, and regional feature restrictions. Existing
serverless-member, global-database, proxy, and connection-activity checks remain.
