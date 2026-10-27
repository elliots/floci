package io.github.hectorvent.floci.runtime.node;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.jboss.logging.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A smolvm-pods node service for tests, on a raw HTTP/1.1 server: the bearer token check, the
 * Docker proxy under {@code /docker} in front of a small in-memory dockerd (images, volumes,
 * containers, execs that succeed, logs), the tunnel ({@code GET /v1/tunnel}) to a container's
 * address, which it relays to the local port the test maps that address to, and
 * {@code GET /v1/pods/ID} for each container, by its id, as the node service has a standalone
 * pod for it, with {@code GET /v1/pods} listing them all, and {@code /v1/network}, which applies every
 * profile. It records every request.
 */
public final class FakeDockerNode implements AutoCloseable {

    /** A request as the fake received it. */
    public record Request(String method, String target, Map<String, String> headers, byte[] body) {

        public String path() {
            int query = target.indexOf('?');
            return query < 0 ? target : target.substring(0, query);
        }

        public JsonNode json() {
            try {
                return JSON.readTree(body);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    private static final Logger LOG = Logger.getLogger(FakeDockerNode.class);
    private static final ObjectMapper JSON = new ObjectMapper();
    /** One docker stats sample of every container: 10% of the CPU, 256 MiB of 1 GiB in use. */
    private static final String STATS = "{\"read\":\"2026-10-25T00:00:01Z\","
            + "\"cpu_stats\":{\"cpu_usage\":{\"total_usage\":2000000000},\"system_cpu_usage\":20000000000,"
            + "\"online_cpus\":2},\"precpu_stats\":{\"cpu_usage\":{\"total_usage\":1000000000},"
            + "\"system_cpu_usage\":10000000000},\"memory_stats\":{\"usage\":268435456,\"limit\":1073741824}}";
    private static final Pattern API_VERSION = Pattern.compile("^/v1\\.\\d+(/.*)$");

    private final String token;
    private final ServerSocket server;
    private final List<Request> requests = new CopyOnWriteArrayList<>();
    private final List<Socket> open = new CopyOnWriteArrayList<>();
    private final Map<String, ObjectNode> containers = new ConcurrentHashMap<>();
    private final Map<String, String> volumes = new ConcurrentHashMap<>();
    private final Map<String, String> podStates = new ConcurrentHashMap<>();
    private final Map<String, Integer> targets = new ConcurrentHashMap<>();
    private final Map<String, List<Socket>> logFollows = new ConcurrentHashMap<>();
    private final AtomicInteger next = new AtomicInteger(1);
    private volatile boolean closed;
    private volatile ObjectNode network = JSON.createObjectNode().put("generation", "");

    /** Starts the fake on {@code port} of 127.0.0.1, or a free port for 0, accepting {@code token}. */
    public FakeDockerNode(String token, int port) throws IOException {
        this.token = token;
        server = new ServerSocket();
        server.setReuseAddress(true);
        server.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), port));
        Thread.ofVirtual().name("fake-docker-node").start(this::accept);
    }

    public URI url() {
        return URI.create("http://127.0.0.1:" + server.getLocalPort());
    }

    public List<Request> requests() {
        return List.copyOf(requests);
    }

    /** The containers dockerd has, by id: the create body, with {@code Id} and {@code Name}. */
    public Map<String, ObjectNode> containers() {
        return Map.copyOf(containers);
    }

    public Map<String, String> volumes() {
        return Map.copyOf(volumes);
    }

    /** Sets the state of the pod of {@code containerId}, such as {@code asleep}. */
    public void podState(String containerId, String state) {
        podStates.put(containerId, state);
    }

    /**
     * Writes a line, stamped with {@code at}, to every log follow of container {@code id}, as
     * dockerd sends a frame of stdout to a follow with timestamps.
     */
    public void logLine(String id, Instant at, String line) throws IOException {
        byte[] payload = (at + " " + line + "\n").getBytes(StandardCharsets.UTF_8);
        byte[] frame = new byte[8 + payload.length];
        frame[0] = 1;
        frame[4] = (byte) (payload.length >>> 24);
        frame[5] = (byte) (payload.length >>> 16);
        frame[6] = (byte) (payload.length >>> 8);
        frame[7] = (byte) payload.length;
        System.arraycopy(payload, 0, frame, 8, payload.length);
        for (Socket socket : logFollows.getOrDefault(id, List.of())) {
            OutputStream output = socket.getOutputStream();
            synchronized (socket) {
                output.write((Integer.toHexString(frame.length) + "\r\n").getBytes(StandardCharsets.ISO_8859_1));
                output.write(frame);
                output.write("\r\n".getBytes(StandardCharsets.ISO_8859_1));
                output.flush();
            }
        }
    }

    /** The number of log follows of container {@code id} that are open. */
    public int logFollows(String id) {
        return logFollows.getOrDefault(id, List.of()).size();
    }

    /** Drops the connection of every log follow, as a connection to the node that dies does. */
    public void dropLogFollows() {
        for (List<Socket> sockets : logFollows.values()) {
            for (Socket socket : sockets) {
                closeQuietly(socket);
            }
        }
    }

    /** Sends a tunnel to {@code ip:port} to {@code localPort} on 127.0.0.1. */
    public void mapTarget(String ip, int port, int localPort) {
        targets.put(ip + ":" + port, localPort);
    }

    @Override
    public void close() {
        closed = true;
        try {
            server.close();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        for (Socket socket : open) {
            closeQuietly(socket);
        }
    }

    private void accept() {
        while (!closed) {
            try {
                Socket socket = server.accept();
                open.add(socket);
                Thread.ofVirtual().start(() -> serve(socket));
            } catch (IOException e) {
                return;
            }
        }
    }

    private void serve(Socket socket) {
        try (socket) {
            InputStream input = socket.getInputStream();
            OutputStream output = socket.getOutputStream();
            while (!closed) {
                Request request = read(input);
                if (request == null) {
                    return;
                }
                requests.add(request);
                if (!("Bearer " + token).equals(request.headers().get("authorization"))) {
                    respond(output, 401, "{\"error\":{\"code\":\"unauthorized\",\"message\":\"no token\",\"reasons\":[]}}");
                    continue;
                }
                if (request.path().equals("/v1/tunnel")) {
                    tunnel(socket, request);
                    return;
                }
                if (!route(socket, output, request)) {
                    return;
                }
            }
        } catch (IOException e) {
            if (!closed) {
                requests.add(new Request("FAILED", e.getMessage(), Map.of(), new byte[0]));
            }
        } finally {
            open.remove(socket);
        }
    }

    /** Answers one request; false when the connection is to close. */
    private boolean route(Socket socket, OutputStream output, Request request) throws IOException {
        String method = request.method();
        String path = request.path();
        if (method.equals("GET") && path.equals("/v1/pods")) {
            ObjectNode answer = JSON.createObjectNode();
            ArrayNode pods = answer.putArray("pods");
            containers.forEach((id, container) -> pods.add(pod(id, container)));
            respond(output, 200, answer.toString());
            return true;
        }
        if (path.equals("/v1/network")) {
            if (method.equals("PUT")) {
                network = JSON.createObjectNode().put("generation", request.json().path("generation").asText())
                        .put("effective_isolation", false).put("node_isolation", false);
            }
            respond(output, 200, network.toString());
            return true;
        }
        Matcher pod = Pattern.compile("^/v1/pods/([^/]+)$").matcher(path);
        if (method.equals("GET") && pod.matches()) {
            String id = URLDecoder.decode(pod.group(1), StandardCharsets.UTF_8);
            ObjectNode container = containers.get(id);
            if (container == null) {
                respond(output, 404, "{\"error\":{\"code\":\"not_found\",\"message\":\"no pod " + id + "\",\"reasons\":[]}}");
            } else {
                respond(output, 200, pod(id, container).toString());
            }
            return true;
        }
        if (!path.startsWith("/docker/")) {
            respond(output, 404, "{\"error\":{\"code\":\"not_found\",\"message\":\"no route\",\"reasons\":[]}}");
            return true;
        }
        String docker = path.substring("/docker".length());
        Matcher versioned = API_VERSION.matcher(docker);
        if (versioned.matches()) {
            docker = versioned.group(1);
        }
        return docker(socket, output, request, method, docker);
    }

    private boolean docker(Socket socket, OutputStream output, Request request, String method, String path)
            throws IOException {
        String[] parts = path.substring(1).split("/");
        String query = request.target().contains("?") ? request.target().substring(request.target().indexOf('?') + 1) : "";
        switch (method + " " + parts[0]) {
            case "GET _ping", "HEAD _ping" -> respond(output, 200, "text/plain", "OK");
            case "GET info" -> respond(output, 200,
                    "{\"Architecture\":\"aarch64\",\"OSType\":\"linux\",\"ServerVersion\":\"27.5.1\"}");
            case "GET images" -> {
                String image = URLDecoder.decode(path.substring("/images/".length(), path.length() - "/json".length()),
                        StandardCharsets.UTF_8);
                ObjectNode answer = JSON.createObjectNode()
                        .put("Id", "sha256:" + sha256(image)).put("Os", "linux").put("Architecture", "arm64");
                answer.putArray("RepoTags").add(image);
                respond(output, 200, answer.toString());
            }
            case "GET volumes" -> {
                String name = parts[1];
                if (volumes.containsKey(name)) {
                    respond(output, 200, JSON.createObjectNode().put("Name", name).put("Driver", "local").toString());
                } else {
                    notFound(output, "no such volume");
                }
            }
            case "POST volumes" -> {
                String name = request.json().path("Name").asText();
                volumes.put(name, request.json().toString());
                respond(output, 201, JSON.createObjectNode().put("Name", name).put("Driver", "local").toString());
            }
            case "DELETE volumes" -> {
                volumes.remove(parts[1]);
                respond(output, 204, "");
            }
            case "GET containers" -> {
                if (parts.length == 2 && parts[1].equals("json")) {
                    ArrayNode list = JSON.createArrayNode();
                    for (ObjectNode container : containers.values()) {
                        ObjectNode summary = list.addObject().put("Id", container.path("Id").asText());
                        summary.putArray("Names").add(container.path("Name").asText());
                    }
                    respond(output, 200, list.toString());
                    return true;
                }
                ObjectNode container = find(parts[1]);
                if (container == null) {
                    notFound(output, "no such container");
                } else if (parts[2].equals("json")) {
                    respond(output, 200, inspect(container).toString());
                } else if (parts[2].equals("logs")) {
                    return logs(socket, output, query, container.path("Id").asText());
                } else if (parts[2].equals("stats")) {
                    respond(output, 200, STATS);
                } else {
                    notFound(output, "no such route");
                }
            }
            case "POST containers" -> {
                if (parts[1].equals("create")) {
                    String name = query.replaceAll(".*name=([^&]+).*", "$1");
                    String id = String.format("%064x", next.getAndIncrement());
                    ObjectNode container = (ObjectNode) request.json();
                    container.put("Id", id).put("Name", "/" + URLDecoder.decode(name, StandardCharsets.UTF_8))
                            .put("Ip", "172.17.0." + (next.get() + 1)).put("Running", false);
                    containers.put(id, container);
                    respond(output, 201, JSON.createObjectNode().put("Id", id).toString());
                    return true;
                }
                ObjectNode container = find(parts[1]);
                if (container == null) {
                    notFound(output, "no such container");
                    return true;
                }
                switch (parts[2]) {
                    case "start" -> container.put("Running", true);
                    case "stop", "kill" -> container.put("Running", false);
                    case "exec" -> {
                        respond(output, 201, JSON.createObjectNode().put("Id", "exec-" + next.getAndIncrement()).toString());
                        return true;
                    }
                    case "wait" -> {
                        respond(output, 200, "{\"StatusCode\":0}");
                        return true;
                    }
                    default -> {
                    }
                }
                respond(output, 204, "");
            }
            case "PUT containers" -> respond(output, 200, "");
            case "DELETE containers" -> {
                ObjectNode container = find(parts[1]);
                if (container == null) {
                    notFound(output, "no such container");
                } else {
                    containers.remove(container.path("Id").asText());
                    respond(output, 204, "");
                }
            }
            case "POST exec" -> respond(output, 200, "application/vnd.docker.multiplexed-stream", "");
            case "GET exec" -> respond(output, 200, "{\"ExitCode\":0,\"Running\":false}");
            default -> notFound(output, "page not found");
        }
        return true;
    }

    /** A follow stays open, as dockerd's does, until the fake closes; any other read is empty. */
    private boolean logs(Socket socket, OutputStream output, String query, String id) throws IOException {
        if (!query.contains("follow=1") && !query.contains("follow=true")) {
            respond(output, 200, "application/vnd.docker.multiplexed-stream", "");
            return true;
        }
        synchronized (socket) {
            output.write(("HTTP/1.1 200 OK\r\nContent-Type: application/vnd.docker.multiplexed-stream\r\n"
                    + "Transfer-Encoding: chunked\r\n\r\n").getBytes(StandardCharsets.ISO_8859_1));
            output.flush();
        }
        List<Socket> follows = logFollows.computeIfAbsent(id, ignored -> new CopyOnWriteArrayList<>());
        follows.add(socket);
        try {
            while (!closed && socket.getInputStream().read() >= 0) {
                Thread.onSpinWait();
            }
        } catch (IOException dropped) {
            // A follow that the test dropped ends here, as its closed connection does.
        } finally {
            follows.remove(socket);
        }
        return false;
    }

    private void tunnel(Socket client, Request request) throws IOException {
        String to = URLDecoder.decode(request.target().replaceAll(".*[?&]to=([^&]+).*", "$1"), StandardCharsets.UTF_8);
        Integer localPort = targets.get(to);
        OutputStream output = client.getOutputStream();
        if (localPort == null) {
            respond(output, 403, "{\"error\":{\"code\":\"forbidden\",\"message\":\"" + to
                    + " is not a container address on the node's Docker networks\",\"reasons\":[]}}");
            return;
        }
        try (Socket target = new Socket(InetAddress.getLoopbackAddress(), localPort)) {
            output.write("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: tcp\r\n\r\n"
                    .getBytes(StandardCharsets.ISO_8859_1));
            output.flush();
            Thread up = Thread.ofVirtual().start(() -> pump(client, target));
            pump(target, client);
            try {
                up.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void pump(Socket from, Socket to) {
        try {
            from.getInputStream().transferTo(to.getOutputStream());
            to.shutdownOutput();
        } catch (IOException e) {
            closeQuietly(to);
        }
    }

    private ObjectNode find(String idOrName) {
        String name = URLDecoder.decode(idOrName, StandardCharsets.UTF_8);
        ObjectNode byId = containers.get(name);
        if (byId != null) {
            return byId;
        }
        for (ObjectNode container : containers.values()) {
            if (container.path("Name").asText().equals("/" + name)) {
                return container;
            }
        }
        return null;
    }

    private ObjectNode pod(String id, ObjectNode container) {
        ObjectNode pod = JSON.createObjectNode()
                .put("sandbox_id", id).put("namespace", "").put("name", container.path("Name").asText().substring(1))
                .put("uid", id).put("ip", container.path("Ip").asText())
                .put("state", podStates.getOrDefault(id, "awake")).put("generation", 1)
                .put("incarnation", "inc-" + id.substring(id.length() - 4)).put("hibernate", true)
                .put("standalone", true);
        pod.putArray("leases");
        pod.putObject("labels");
        pod.putNull("controller");
        return pod;
    }

    private static ObjectNode inspect(ObjectNode container) {
        ObjectNode inspect = JSON.createObjectNode()
                .put("Id", container.path("Id").asText()).put("Name", container.path("Name").asText())
                .put("Image", container.path("Image").asText());
        boolean running = container.path("Running").asBoolean();
        inspect.putObject("State").put("Running", running).put("Paused", false)
                .put("Status", running ? "running" : "exited");
        inspect.set("HostConfig", container.path("HostConfig"));
        ObjectNode network = inspect.putObject("NetworkSettings");
        network.put("IPAddress", container.path("Ip").asText());
        network.putObject("Ports");
        network.putObject("Networks").putObject("bridge").put("IPAddress", container.path("Ip").asText());
        return inspect;
    }

    private static Request read(InputStream input) throws IOException {
        ByteArrayOutputStream head = new ByteArrayOutputStream();
        while (true) {
            int b = input.read();
            if (b < 0) {
                return null;
            }
            head.write(b);
            byte[] bytes = head.toByteArray();
            int n = bytes.length;
            if (n >= 4 && bytes[n - 4] == '\r' && bytes[n - 3] == '\n' && bytes[n - 2] == '\r' && bytes[n - 1] == '\n') {
                break;
            }
        }
        String[] lines = head.toString(StandardCharsets.ISO_8859_1).split("\r\n");
        String[] requestLine = lines[0].split(" ");
        Map<String, String> headers = new LinkedHashMap<>();
        for (int i = 1; i < lines.length; i++) {
            int colon = lines[i].indexOf(':');
            if (colon > 0) {
                headers.put(lines[i].substring(0, colon).trim().toLowerCase(Locale.ROOT), lines[i].substring(colon + 1).trim());
            }
        }
        byte[] body = new byte[0];
        if (headers.containsKey("content-length")) {
            body = input.readNBytes(Integer.parseInt(headers.get("content-length")));
        } else if ("chunked".equalsIgnoreCase(headers.get("transfer-encoding"))) {
            ByteArrayOutputStream chunks = new ByteArrayOutputStream();
            while (true) {
                String size = line(input);
                int length = Integer.parseInt(size.split(";")[0].trim(), 16);
                if (length == 0) {
                    line(input);
                    break;
                }
                chunks.write(input.readNBytes(length));
                line(input);
            }
            body = chunks.toByteArray();
        }
        return new Request(requestLine[0], requestLine[1], headers, body);
    }

    private static String line(InputStream input) throws IOException {
        ByteArrayOutputStream line = new ByteArrayOutputStream();
        int b;
        while ((b = input.read()) >= 0 && b != '\n') {
            if (b != '\r') {
                line.write(b);
            }
        }
        return line.toString(StandardCharsets.ISO_8859_1);
    }

    private static void notFound(OutputStream output, String message) throws IOException {
        respond(output, 404, JSON.createObjectNode().put("message", message).toString());
    }

    private static void respond(OutputStream output, int status, String body) throws IOException {
        respond(output, status, "application/json", body);
    }

    private static void respond(OutputStream output, int status, String contentType, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        String head = "HTTP/1.1 " + status + " " + (status < 300 ? "OK" : "Error") + "\r\n"
                + (status == 204 ? "" : "Content-Type: " + contentType + "\r\nContent-Length: " + bytes.length + "\r\n")
                + "\r\n";
        output.write(head.getBytes(StandardCharsets.ISO_8859_1));
        if (status != 204) {
            output.write(bytes);
        }
        output.flush();
    }

    private static String sha256(String text) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(text.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException e) {
            LOG.debugv("Closing a socket of the fake node: {0}", e.getMessage());
        }
    }
}
