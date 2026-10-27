package io.github.hectorvent.floci.services.amazonmq;

import io.github.hectorvent.floci.services.amazonmq.model.Broker;

/**
 * Hears when a broker that Floci runs in Docker becomes {@code RUNNING} and when a broker is
 * deleted. Implementations return at once: they run on Amazon MQ's own threads.
 */
public interface BrokerListener {

    /** The broker's container answers, and the broker is {@code RUNNING}. */
    void brokerRunning(Broker broker);

    /** The broker was deleted, and its container with it. */
    void brokerDeleted(Broker broker);
}
