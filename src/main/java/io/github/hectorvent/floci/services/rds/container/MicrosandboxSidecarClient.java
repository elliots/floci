package io.github.hectorvent.floci.services.rds.container;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.core.common.microsandbox.MicrosandboxClient;
import io.github.hectorvent.floci.core.common.microsandbox.MicrosandboxOperationJournal;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.time.Duration;
import java.util.List;
import java.util.function.BooleanSupplier;

@ApplicationScoped
public class MicrosandboxSidecarClient {
    private static final Duration TIMEOUT = Duration.ofSeconds(195);
    private final MicrosandboxSidecarManager manager;
    private final ObjectMapper mapper;
    private final MicrosandboxOperationJournal operations;
    private MicrosandboxClient transport;

    public MicrosandboxSidecarClient(MicrosandboxSidecarManager manager, ObjectMapper mapper) {
        this(manager, mapper, null);
    }

    @Inject
    public MicrosandboxSidecarClient(MicrosandboxSidecarManager manager, ObjectMapper mapper,
                                     MicrosandboxOperationJournal operations) {
        this.manager = manager;
        this.mapper = mapper;
        this.operations = operations;
    }

    private synchronized MicrosandboxClient transport() {
        if (transport == null) {
            transport = new MicrosandboxClient(manager.url(), manager.token(), mapper, operations);
            transport.requireNetworkPolicy(manager.networkPolicy());
        }
        return transport;
    }

    int port() {
        return transport().port(TIMEOUT);
    }

    String command(String... arguments) {
        return transport().command(TIMEOUT, arguments);
    }

    String commandWithVerifiedCompletion(BooleanSupplier completed, String... arguments) {
        return transport().commandWithVerifiedCompletion(TIMEOUT, completed, arguments);
    }

    String execute(List<String> arguments, String input) {
        return transport().execute(TIMEOUT, arguments, input);
    }

    JsonNode json(String... arguments) {
        return transport().json(TIMEOUT, arguments);
    }

    void awaitQuiescence(Duration timeout) {
        transport().awaitQuiescence(timeout);
    }
}
