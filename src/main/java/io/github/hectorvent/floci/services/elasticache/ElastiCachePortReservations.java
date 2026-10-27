package io.github.hectorvent.floci.services.elasticache;

import jakarta.enterprise.context.ApplicationScoped;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Port reservations shared by Redis/Valkey proxies and Memcached node relays. */
@ApplicationScoped
public class ElastiCachePortReservations {
    private final Set<Integer> ports = ConcurrentHashMap.newKeySet();

    public boolean claim(int port) {
        return ports.add(port);
    }

    public void release(int port) {
        ports.remove(port);
    }
}
