package io.github.hectorvent.floci.runtime.ondemand;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.config.NetworkExposureGuard;
import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.core.common.RequestScopes;
import io.github.hectorvent.floci.core.common.Resettable;
import io.github.hectorvent.floci.services.sqs.QueueActivity;
import io.github.hectorvent.floci.services.sqs.SqsService;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.vertx.core.Vertx;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.interceptor.Interceptor;
import org.jboss.logging.Logger;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@ApplicationScoped
public class OnDemandService implements Resettable {
    private static final Logger LOG = Logger.getLogger(OnDemandService.class);
    private static final List<String> QUEUE_ATTRIBUTES = List.of(
            "ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible");

    private final EmulatorConfig config;
    private final Instance<SqsService> sqs;
    private final WorkloadRuntimeFactory runtimes;
    private final Vertx vertx;
    private final Map<String, AtomicBoolean> reconciling = new ConcurrentHashMap<>();
    private List<WorkloadDefinition> definitions = List.of();
    private volatile ActivationCoordinator coordinator;
    private volatile ExecutorService reconciliationWorkers;
    private ScheduledExecutorService scheduler;
    private OnDemandGateway gateway;

    @Inject
    public OnDemandService(EmulatorConfig config, Instance<SqsService> sqs,
                            WorkloadRuntimeFactory runtimes, Vertx vertx) {
        this.config = config;
        this.sqs = sqs;
        this.runtimes = runtimes;
        this.vertx = vertx;
    }

    void start(@Observes @Priority(Interceptor.Priority.APPLICATION + 100) StartupEvent ignored) {
        if (!config.onDemand().enabled()) {
            return;
        }
        validateSettings();
        definitions = WorkloadConfigLoader.load(Path.of(config.onDemand().configFile()
                .orElseThrow(() -> new IllegalArgumentException("floci.on-demand.config-file is required"))),
                config.defaultAccountId(), config.defaultRegion());
        startCoordinator();
        gateway = new OnDemandGateway(vertx, definitions, () -> coordinator);
        try {
            gateway.start(config.onDemand().gatewayHost(), config.onDemand().gatewayPort())
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            gateway.close();
            stopRuntime();
            throw new IllegalStateException("Cannot start on-demand application gateway", e);
        }
        LOG.infov("On-demand application gateway listening on {0}:{1}, workloads={2}",
                config.onDemand().gatewayHost(), gateway.port(), definitions.size());
    }

    private void validateSettings() {
        EmulatorConfig.OnDemandConfig settings = config.onDemand();
        NetworkExposureGuard.requireConsent(settings.gatewayHost(), config.security());
        if (settings.gatewayPort() < 0 || settings.gatewayPort() > 65535 || settings.gatewayPort() == config.port()
                || settings.reconcileIntervalMillis() < 100 || settings.maxConcurrentStarts() < 1
                || settings.maxPendingRequests() < 1 || settings.maxPendingRequestsPerWorkload() < 1) {
            throw new IllegalArgumentException("Invalid floci.on-demand gateway or capacity settings");
        }
    }

    private void startCoordinator() {
        EmulatorConfig.OnDemandConfig settings = config.onDemand();
        ActivationCoordinator current = new ActivationCoordinator(
                Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("on-demand-runtime-", 0).factory()),
                new HttpReadiness(), settings.maxConcurrentStarts(), settings.maxPendingRequests(),
                settings.maxPendingRequestsPerWorkload(), System::nanoTime);
        for (WorkloadDefinition definition : definitions) {
            current.register(definition, runtimes.create(definition));
        }
        coordinator = current;
        reconciliationWorkers = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("on-demand-queues-", 0).factory());
        scheduler = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon()
                .name("on-demand-reconcile").factory());
        scheduler.scheduleWithFixedDelay(this::reconcileAll, 0, settings.reconcileIntervalMillis(), TimeUnit.MILLISECONDS);
    }

    private void reconcileAll() {
        for (WorkloadDefinition definition : definitions) {
            if (definition.enabled()) {
                schedule(definition);
            }
        }
    }

    ActivationCoordinator coordinator() {
        return coordinator;
    }

    void queued(@Observes QueueActivity activity) {
        if (coordinator == null) {
            return;
        }
        for (WorkloadDefinition definition : definitions) {
            if (definition.enabled() && definition.accountId().equals(activity.accountId())
                    && definition.region().equals(activity.region()) && definition.queues().contains(activity.queueName())) {
                schedule(definition);
            }
        }
    }

    private void schedule(WorkloadDefinition definition) {
        ExecutorService executor = reconciliationWorkers;
        ActivationCoordinator current = coordinator;
        if (executor == null || executor.isShutdown() || current == null) {
            return;
        }
        AtomicBoolean inProgress = reconciling.computeIfAbsent(definition.id(), ignored -> new AtomicBoolean());
        if (!inProgress.compareAndSet(false, true)) {
            return;
        }
        try {
            executor.execute(() -> {
                try {
                    boolean active = RequestScopes.callAs(definition.accountId(), definition.region(),
                            () -> hasQueueWork(definition));
                    current.reconcile(definition.id(), active, true);
                } catch (RuntimeException e) {
                    // A failed queue read must not allow a potentially busy worker to stop.
                    current.reconcile(definition.id(), false, false);
                    LOG.debugv("On-demand reconciliation failed for {0}: {1}", definition.id(), e.getMessage());
                } finally {
                    inProgress.set(false);
                }
            });
        } catch (RuntimeException e) {
            inProgress.set(false);
            if (!executor.isShutdown()) {
                LOG.warnv(e, "Could not schedule on-demand reconciliation for {0}", definition.id());
            }
        }
    }

    private boolean hasQueueWork(WorkloadDefinition definition) {
        boolean active = false;
        for (String queueName : definition.queues()) {
            try {
                SqsService service = sqs.get();
                String url = service.getQueueUrl(queueName, definition.region());
                Map<String, String> attributes = service.getQueueAttributes(url, QUEUE_ATTRIBUTES, definition.region());
                active |= Long.parseLong(attributes.get("ApproximateNumberOfMessages")) > 0
                        || Long.parseLong(attributes.get("ApproximateNumberOfMessagesNotVisible")) > 0;
            } catch (AwsException e) {
                if (!"AWS.SimpleQueueService.NonExistentQueue".equals(e.getErrorCode())) {
                    throw e;
                }
                LOG.tracev("On-demand queue {0} does not exist yet", queueName);
            }
        }
        return active;
    }

    void stop(@Observes @Priority(Interceptor.Priority.APPLICATION - 100) ShutdownEvent ignored) {
        if (gateway != null) {
            gateway.close();
        }
        try {
            stopRuntime();
        } catch (RuntimeException e) {
            LOG.warnv(e, "On-demand shutdown did not finish; continuing with emulator resource cleanup");
        }
    }

    private void stopRuntime() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        ExecutorService workers = reconciliationWorkers;
        reconciliationWorkers = null;
        if (workers != null) {
            workers.shutdownNow();
        }
        ActivationCoordinator current = coordinator;
        if (current != null) {
            current.close();
        }
        coordinator = null;
        if (workers != null) {
            try {
                if (!workers.awaitTermination(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("On-demand queue reads did not stop before resource reset");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted stopping on-demand queue reads", e);
            }
        }
        reconciling.clear();
    }

    @Override
    public void beforeReset() {
        stopRuntime();
    }

    @Override
    public void clear() {
        // Definitions remain in the external configuration; resource state is owned by StorageFactory.
        runtimes.closeManagedClients();
    }

    @Override
    public void afterReset() {
        if (config.onDemand().enabled() && coordinator == null) {
            startCoordinator();
        }
    }
}
