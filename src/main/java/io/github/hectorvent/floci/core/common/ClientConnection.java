package io.github.hectorvent.floci.core.common;

import io.quarkus.vertx.http.runtime.CurrentVertxRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.function.BooleanSupplier;

/**
 * Whether the client of a request that waits, such as a long poll, has closed its connection, so
 * that the request stops instead of taking work for a client that is gone.
 */
@ApplicationScoped
public class ClientConnection {

    private final CurrentVertxRequest currentVertxRequest;

    public ClientConnection(CurrentVertxRequest currentVertxRequest) {
        this.currentVertxRequest = currentVertxRequest;
    }

    /**
     * A check, safe to call from any thread, of whether the current request's connection is closed.
     * Outside an HTTP request it is never closed.
     */
    public BooleanSupplier closedCheck() {
        RoutingContext context = currentVertxRequest.getCurrent();
        if (context == null) {
            return () -> false;
        }
        HttpServerResponse response = context.response();
        return response::closed;
    }
}
