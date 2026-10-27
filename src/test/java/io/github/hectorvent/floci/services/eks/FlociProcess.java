package io.github.hectorvent.floci.services.eks;

import org.jboss.logging.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Floci as a child process, for tests that stop and start it: the packaged JVM build in
 * {@code target/quarkus-app}, or the binary that {@code FLOCI_EKS_SMOLVM_E2E_FLOCI} names. It runs
 * in a working directory of its own, with no {@code FLOCI_*}, {@code QUARKUS_*} or {@code AWS_*}
 * variable of the test's environment, and writes each run's output to {@code floci-N.log} there.
 */
public final class FlociProcess implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(FlociProcess.class);

    /** The native binary or launcher to run in place of the packaged JVM build. */
    static final String COMMAND_VARIABLE = "FLOCI_EKS_SMOLVM_E2E_FLOCI";

    private static final Duration START_TIMEOUT = Duration.ofSeconds(90);
    private static final Duration STOP_TIMEOUT = Duration.ofSeconds(120);

    private final Path workDir;
    private final int port;
    private final List<String> command;
    private final HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(2)).build();
    private Process process;
    private Path log;
    private int runs;

    private FlociProcess(Path workDir, int port, List<String> command) {
        this.workDir = workDir;
        this.port = port;
        this.command = command;
    }

    /**
     * Floci on {@code port}, run in {@code workDir}.
     *
     * @param moduleDir the Maven module, whose {@code target/quarkus-app} holds the JVM build
     * @throws IllegalStateException when there is no build to run, or the JVM build is older than
     *                               the compiled classes
     */
    public static FlociProcess of(Path moduleDir, Path workDir, int port) {
        String binary = System.getenv(COMMAND_VARIABLE);
        if (binary != null && !binary.isBlank()) {
            return new FlociProcess(workDir, port, List.of(binary));
        }
        Path jar = moduleDir.resolve("target/quarkus-app/quarkus-run.jar");
        if (!Files.isRegularFile(jar)) {
            throw new IllegalStateException("No Floci build at " + jar + ": run ./mvnw package -DskipTests first, "
                    + "or name a binary in " + COMMAND_VARIABLE);
        }
        Instant built = newest(moduleDir.resolve("target/quarkus-app/app"));
        Instant compiled = newest(moduleDir.resolve("target/classes/io"));
        if (built.isBefore(compiled)) {
            throw new IllegalStateException("The Floci build in target/quarkus-app is older than target/classes: "
                    + "run ./mvnw package -DskipTests again");
        }
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        return new FlociProcess(workDir, port, List.of(java, "-jar", jar.toString()));
    }

    int port() {
        return port;
    }

    public URI endpoint() {
        return URI.create("http://localhost:" + port);
    }

    /** Starts Floci with {@code env} and waits until it answers its health check. */
    public void start(Map<String, String> env) {
        if (process != null && process.isAlive()) {
            throw new IllegalStateException("Floci already runs");
        }
        runs++;
        log = workDir.resolve("floci-" + runs + ".log");
        ProcessBuilder builder = new ProcessBuilder(command).directory(workDir.toFile())
                .redirectErrorStream(true).redirectOutput(log.toFile());
        Map<String, String> environment = builder.environment();
        environment.keySet().removeIf(name -> name.startsWith("FLOCI_") || name.startsWith("QUARKUS_")
                || name.startsWith("AWS_"));
        environment.putAll(env);
        environment.put("FLOCI_PORT", String.valueOf(port));
        try {
            process = builder.start();
        } catch (IOException e) {
            throw new UncheckedIOException("Could not start Floci with " + command, e);
        }
        Instant deadline = Instant.now().plus(START_TIMEOUT);
        while (Instant.now().isBefore(deadline)) {
            if (!process.isAlive()) {
                throw new IllegalStateException("Floci exited with " + process.exitValue() + ":\n" + logTail(60));
            }
            if (healthy()) {
                LOG.infov("Floci run {0} answers on port {1}, logging to {2}", String.valueOf(runs),
                        String.valueOf(port), log);
                return;
            }
            pause(Duration.ofMillis(250));
        }
        throw new IllegalStateException("Floci did not answer within " + START_TIMEOUT.toSeconds() + " s:\n"
                + logTail(60));
    }

    /** Stops Floci as SIGTERM does, so its shutdown runs, and waits until it exits. */
    public void stop() {
        if (process == null || !process.isAlive()) {
            return;
        }
        process.destroy();
        try {
            if (!process.waitFor(STOP_TIMEOUT.toSeconds(), TimeUnit.SECONDS)) {
                process.destroyForcibly();
                throw new IllegalStateException("Floci did not stop within " + STOP_TIMEOUT.toSeconds() + " s:\n"
                        + logTail(60));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
            throw new IllegalStateException("Interrupted while Floci stopped", e);
        }
        LOG.infov("Floci run {0} exited with {1}", String.valueOf(runs), String.valueOf(process.exitValue()));
    }

    /** The last {@code lines} lines of the current run's log. */
    public String logTail(int lines) {
        if (log == null || !Files.exists(log)) {
            return "(no log)";
        }
        try {
            List<String> all = Files.readAllLines(log);
            return String.join("\n", all.subList(Math.max(0, all.size() - lines), all.size()));
        } catch (IOException e) {
            return "(the log " + log + " cannot be read: " + e.getMessage() + ")";
        }
    }

    /** Every log line of every run so far that contains {@code text}. */
    public List<String> logLines(String text) {
        List<String> found = new ArrayList<>();
        for (int run = 1; run <= runs; run++) {
            Path file = workDir.resolve("floci-" + run + ".log");
            try (Stream<String> lines = Files.lines(file)) {
                lines.filter(line -> line.contains(text)).forEach(found::add);
            } catch (IOException | UncheckedIOException e) {
                LOG.warnv("Could not read {0}: {1}", file, e.getMessage());
            }
        }
        return found;
    }

    @Override
    public void close() {
        stop();
    }

    private boolean healthy() {
        HttpRequest request = HttpRequest.newBuilder(endpoint().resolve("/_floci/health"))
                .timeout(Duration.ofSeconds(2)).GET().build();
        try {
            return http.send(request, HttpResponse.BodyHandlers.discarding()).statusCode() == 200;
        } catch (IOException e) {
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while Floci started", e);
        }
    }

    /** The newest modification time of the files under {@code dir}. */
    private static Instant newest(Path dir) {
        if (!Files.isDirectory(dir)) {
            return Instant.EPOCH;
        }
        try (Stream<Path> files = Files.walk(dir)) {
            return files.filter(Files::isRegularFile).map(FlociProcess::modified).max(Instant::compareTo)
                    .orElse(Instant.EPOCH);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Instant modified(Path file) {
        try {
            return Files.getLastModifiedTime(file).toInstant();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void pause(Duration duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted", e);
        }
    }
}
