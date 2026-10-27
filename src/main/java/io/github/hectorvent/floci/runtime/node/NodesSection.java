package io.github.hectorvent.floci.runtime.node;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;

/**
 * The runtime file's {@code nodes}: smolvm-pods nodes that Floci did not create, by key.
 *
 * <pre>
 * nodes:
 *   dev:
 *     url: http://127.0.0.1:17070
 *     token-file: /Users/me/.floci/dev-node-token
 * </pre>
 */
@ApplicationScoped
public class NodesSection implements RuntimeFileSection<Map<String, SmolvmNode>> {

    public static final String NAME = "nodes";

    private static final Pattern KEY = Pattern.compile("[A-Za-z0-9_-]{1,63}");
    private static final List<String> FIELDS = List.of("url", "token-file", "token-env");

    private final UnaryOperator<String> environment;

    @Inject
    public NodesSection() {
        this(NodeToken.TokenVariable.PROCESS_ENVIRONMENT);
    }

    NodesSection(UnaryOperator<String> environment) {
        this.environment = environment;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Map<String, SmolvmNode> empty() {
        return Map.of();
    }

    @Override
    public Map<String, SmolvmNode> read(JsonNode node, Path file) {
        RuntimeFileFields.mapping(node, NAME);
        Map<String, SmolvmNode> nodes = new LinkedHashMap<>();
        for (Map.Entry<String, JsonNode> entry : node.properties()) {
            String key = entry.getKey();
            String path = RuntimeFileFields.path(NAME, key);
            if (!KEY.matcher(key).matches()) {
                throw new RuntimeFileException(path, "a node name is 1 to 63 of A-Z, a-z, 0-9, _ and -");
            }
            nodes.put(key, node(key, entry.getValue(), path, file));
        }
        return Collections.unmodifiableMap(nodes);
    }

    private SmolvmNode node(String key, JsonNode node, String path, Path file) {
        RuntimeFileFields.mapping(node, path, FIELDS);
        URI url = url(RuntimeFileFields.text(node, path, "url"), RuntimeFileFields.path(path, "url"));
        String tokenFile = RuntimeFileFields.optionalText(node, path, "token-file");
        String tokenEnv = RuntimeFileFields.optionalText(node, path, "token-env");
        if ((tokenFile == null) == (tokenEnv == null)) {
            throw new RuntimeFileException(path, "needs exactly one of token-file and token-env");
        }
        NodeToken token;
        String tokenPath;
        if (tokenFile != null) {
            tokenPath = RuntimeFileFields.path(path, "token-file");
            token = NodeToken.file(tokenFile(tokenFile, tokenPath, file));
        } else {
            tokenPath = RuntimeFileFields.path(path, "token-env");
            token = new NodeToken.TokenVariable(tokenEnv, environment);
        }
        try {
            token.read();
        } catch (IllegalStateException e) {
            throw new RuntimeFileException(tokenPath, e.getMessage());
        }
        return new SmolvmNode(SmolvmNode.declaredName(key), url, token, SmolvmNode.Kind.DECLARED, List.of());
    }

    private static URI url(String value, String path) {
        RuntimeFileException invalid = new RuntimeFileException(path,
                "must be http:// or https:// with a host and a port, and nothing else");
        URI uri;
        try {
            uri = new URI(value);
        } catch (URISyntaxException e) {
            throw invalid;
        }
        String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
        if ((!scheme.equals("http") && !scheme.equals("https")) || uri.getHost() == null
                || uri.getPort() < 1 || uri.getPort() > 65535 || uri.getRawUserInfo() != null
                || uri.getRawQuery() != null || uri.getRawFragment() != null
                || !(uri.getRawPath() == null || uri.getRawPath().isEmpty() || uri.getRawPath().equals("/"))) {
            throw invalid;
        }
        return URI.create(scheme + "://" + uri.getRawAuthority());
    }

    private static Path tokenFile(String value, String path, Path file) {
        try {
            Path token = Path.of(value);
            return token.isAbsolute() ? token : file.toAbsolutePath().resolveSibling(token).normalize();
        } catch (InvalidPathException e) {
            throw new RuntimeFileException(path, "is not a path: " + e.getReason());
        }
    }
}
