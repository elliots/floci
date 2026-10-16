package io.github.hectorvent.floci.services.rds.container;

import io.github.hectorvent.floci.core.common.Resettable;
import io.github.hectorvent.floci.core.common.docker.ContainerLiveness;
import io.github.hectorvent.floci.services.rds.model.DatabaseEngine;

/**
 * Database execution boundary used by RDS, its connection gate, and emulator shutdown.
 * AWS resource state and eligibility for auto-pause remain in the RDS service.
 *
 * <p>The existing handle and method names are retained to avoid changing persisted identities
 * and callers together with the runtime seam. Container IDs and storage names are opaque to
 * callers: a provider must resolve them to its current execution generation. A suspended
 * backend retains its identity and endpoint and must not be reported as missing.
 *
 * <p>A replacement must serve this interface and {@link RdsBackendGate} from the same CDI bean,
 * so maintenance operations, TCP clients, and Data API transactions share activity leases.
 */
public interface RdsBackendRuntime extends RdsBackendGate, Resettable {

    /**
     * Starts a database, retaining cleanup identity on partial failure. Returns null only when
     * the execution provider is unreachable, preserving metadata-only operation.
     * The storage identity must never be silently redirected to an empty database.
     */
    RdsContainerHandle tryStart(
            String runtimeId, String instanceId, String storageResourceId,
            String storageName, DatabaseEngine engine, String image,
            String masterUsername, String masterPassword, String dbName);

    /** Legacy name for provider reachability; an implementation need not use Docker. */
    boolean isDockerReachable();

    /** Stops execution without deleting durable database storage. */
    void stop(RdsContainerHandle handle);

    void stopByRuntimeId(String runtimeId);

    /** UNKNOWN is a failed probe, not evidence that the backend has disappeared. */
    ContainerLiveness probeContainer(String backendId);

    RdsContainerHandle getActiveHandle(String runtimeId);

    void stopAll();

    /** Null disables auto-pause; existing leases must prevent suspension. */
    void configureAutoPause(String runtimeId, Integer secondsUntilAutoPause, AutoPauseListener listener);

    void rotateMasterPassword(String backendName, String backendId, DatabaseEngine engine,
                              String masterUsername, String oldPassword, String newPassword);

    /** Logical database backup for the AWS snapshot API, distinct from a runtime checkpoint. */
    String createPostgresSnapshot(String backendId, String masterUsername);

    void restorePostgresSnapshot(String backendId, String masterUsername, String sqlDump);

    void renamePostgresDatabase(String backendId, String masterUsername, String from, String to);

    /** Deletes storage only after execution has stopped and ownership has been released. */
    void removeVolume(String runtimeId, String storageResourceId, String storageName);
}
