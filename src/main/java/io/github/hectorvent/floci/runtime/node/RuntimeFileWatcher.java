package io.github.hectorvent.floci.runtime.node;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.interceptor.Interceptor;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Holds the definitions of the runtime file that {@code floci.runtime.config-file} names. Startup
 * fails when the file is missing or invalid. Every {@link #CHECK_INTERVAL} the file is read again
 * if its modification time or size changed, or while it is invalid: a valid file replaces every
 * section at once and the listeners are told; an invalid one is logged and the definitions in
 * force stay.
 */
@ApplicationScoped
public class RuntimeFileWatcher {

    static final Duration CHECK_INTERVAL = Duration.ofSeconds(2);

    private static final Logger LOG = Logger.getLogger(RuntimeFileWatcher.class);

    private final Path file;
    private final List<RuntimeFileSection<?>> sections;
    private final List<Consumer<RuntimeFile>> listeners = new CopyOnWriteArrayList<>();
    private volatile RuntimeFile current;
    private Stamp seen;
    /** The problem of the last read, or null when it succeeded. */
    private String problem;
    private ScheduledExecutorService scheduler;

    @Inject
    public RuntimeFileWatcher(EmulatorConfig config, Instance<RuntimeFileSection<?>> sections) {
        this(config.runtime().configFile().filter(path -> !path.isBlank()).map(Path::of).orElse(null),
                sections.stream().toList());
    }

    /**
     * @param file     the runtime file, or null when none is configured
     * @param sections the registered sections, each with a distinct name of {@link RuntimeFile#SECTIONS}
     */
    RuntimeFileWatcher(Path file, List<RuntimeFileSection<?>> sections) {
        Set<String> names = new HashSet<>();
        for (RuntimeFileSection<?> section : sections) {
            if (!RuntimeFile.SECTIONS.contains(section.name()) || !names.add(section.name())) {
                throw new IllegalStateException("runtime file section " + section.name()
                        + " is registered twice or is not one of " + RuntimeFile.SECTIONS);
            }
        }
        this.file = file;
        this.sections = sections.stream()
                .sorted(Comparator.comparingInt(section -> RuntimeFile.SECTIONS.indexOf(section.name())))
                .toList();
    }

    void onStart(@Observes @Priority(Interceptor.Priority.LIBRARY_BEFORE) StartupEvent ignored) {
        current();
        if (file != null) {
            scheduler = Executors.newSingleThreadScheduledExecutor(
                    Thread.ofPlatform().daemon().name("runtime-file-watcher").factory());
            scheduler.scheduleWithFixedDelay(this::checkLogged, CHECK_INTERVAL.toMillis(), CHECK_INTERVAL.toMillis(),
                    TimeUnit.MILLISECONDS);
        }
    }

    void onStop(@Observes ShutdownEvent ignored) {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    /**
     * The definitions in force: the file's, or empty ones when no file is configured.
     *
     * @throws IllegalStateException on the first call, when the file is missing or invalid
     */
    public RuntimeFile current() {
        RuntimeFile loaded = current;
        if (loaded != null) {
            return loaded;
        }
        synchronized (this) {
            if (current == null) {
                current = load();
            }
            return current;
        }
    }

    /** Whether {@code floci.runtime.config-file} names a runtime file. */
    public boolean configured() {
        return file != null;
    }

    /** Tells {@code listener} each new set of definitions, on the watcher's thread. */
    public void addListener(Consumer<RuntimeFile> listener) {
        listeners.add(listener);
    }

    public void removeListener(Consumer<RuntimeFile> listener) {
        listeners.remove(listener);
    }

    /**
     * Reads the file again when its modification time or size changed since the last read, and
     * while the last read failed, since a fix may be outside the file, such as in a token file it
     * names. A problem is logged when the file changes or the problem does.
     */
    synchronized void check() {
        RuntimeFile inForce = current();
        if (file == null) {
            return;
        }
        Stamp stamp = Stamp.of(file);
        boolean changed = !stamp.equals(seen);
        if (!changed && problem == null) {
            return;
        }
        seen = stamp;
        RuntimeFile next;
        try {
            next = RuntimeFile.read(file, sections);
        } catch (RuntimeFileException e) {
            if (changed || !e.getMessage().equals(problem)) {
                LOG.errorv("The runtime file {0} is invalid, so the definitions in force stay: {1}", file,
                        e.getMessage());
            }
            problem = e.getMessage();
            return;
        }
        boolean fixed = problem != null;
        problem = null;
        if (next.equals(inForce)) {
            if (fixed) {
                LOG.infov("The runtime file {0} is valid again, with the definitions in force", file);
            } else {
                LOG.debugv("The runtime file {0} changed, but not its definitions", file);
            }
            return;
        }
        current = next;
        LOG.infov("Read the runtime file {0} again", file);
        for (Consumer<RuntimeFile> listener : listeners) {
            try {
                listener.accept(next);
            } catch (RuntimeException e) {
                LOG.errorv(e, "A listener of the runtime file {0} failed", file);
            }
        }
    }

    private void checkLogged() {
        try {
            check();
        } catch (RuntimeException e) {
            LOG.errorv(e, "Checking the runtime file {0} failed", file);
        }
    }

    private RuntimeFile load() {
        if (file == null) {
            return RuntimeFile.empty(sections);
        }
        seen = Stamp.of(file);
        try {
            RuntimeFile loaded = RuntimeFile.read(file, sections);
            LOG.infov("Read the runtime file {0}", file);
            return loaded;
        } catch (RuntimeFileException e) {
            throw new IllegalStateException("The runtime file " + file + " (FLOCI_RUNTIME_CONFIG_FILE) is invalid: "
                    + e.getMessage(), e);
        }
    }

    /** The modification time and size of a file, or neither when it cannot be read. */
    private record Stamp(FileTime modified, long size) {

        static Stamp of(Path file) {
            try {
                BasicFileAttributes attributes = Files.readAttributes(file, BasicFileAttributes.class);
                return new Stamp(attributes.lastModifiedTime(), attributes.size());
            } catch (IOException unreadable) {
                // Compares as missing; the read that follows reports why.
                return new Stamp(null, -1);
            }
        }
    }
}
