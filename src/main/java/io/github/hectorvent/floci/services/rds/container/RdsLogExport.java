package io.github.hectorvent.floci.services.rds.container;

import io.github.hectorvent.floci.core.common.docker.ContainerLogStreamer.LogDestination;

import java.util.List;

/**
 * Where the output of a database container goes in CloudWatch Logs: the log groups and streams of
 * the log types its instance or cluster exports, in the account and region that own it. No
 * destination ships it to the Floci console only.
 */
public record RdsLogExport(String accountId, String region, List<LogDestination> destinations) {

    public RdsLogExport {
        destinations = List.copyOf(destinations);
    }
}
