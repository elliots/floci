# Native Aurora recovery and disk sizing

Existing PostgreSQL and MySQL native runtimes now recover their stable endpoints
without eagerly starting every database when Floci restarts. A stopped guest or
hibernated checkpoint remains cold until a database connection, Data API request,
or maintenance operation needs it. Metadata reads do not wake it. Guests already
running in the external helper continue running.

Ordinary Floci shutdown now retains verified hibernated guests and their RAM
checkpoints. Running databases still receive a clean shutdown. AWS
`StopDBInstance`, `StopDBCluster`, and their start operations keep their existing
behavior: explicit stop shuts down the database, and explicit start boots fresh
processes using its preserved disk.

Keep Floci's persistent storage and its dedicated helper runtime home together.
Recovery requires an unambiguous `rds-microsandbox.json` journal and owned guest
storage. Missing or uncertain state is reported as a recoverable backend failure;
Floci does not initialize a new empty database over lost state. Restoring a
backup of metadata alone is not a database recovery procedure.

New native databases use
`floci.services.rds.microsandbox-root-disk-mib`, or
`FLOCI_SERVICES_RDS_MICROSANDBOX_ROOT_DISK_MIB`. It defaults to 4096 MiB and accepts
sizes of at least 1024 MiB. This disk contains both the image filesystem and the
database's data. Allow room for both when choosing a size.

The setting is recorded when a native database is created. Existing databases
retain their recorded disk size when configuration changes, including after
cold start or RAM restoration. Journals created before this setting existed keep
their original 4096 MiB disk size. There is no online grow or shrink operation;
create another database and migrate data to change its size.

This is a local runtime configuration setting. AWS `AllocatedStorage` request and
response semantics have not changed, and modifying it does not resize a native
disk. Docker database behavior and its existing volumes are unchanged.
