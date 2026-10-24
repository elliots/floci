package io.github.hectorvent.floci.runtime.node;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RuntimeFileTest {

    private static final String TOKEN = "0123456789abcdef0123456789abcdef";

    @TempDir
    Path dir;

    private final Map<String, String> environment = new HashMap<>();
    private final NodesSection nodes = new NodesSection(environment::get);

    @Test
    void readsANodeWithATokenFile() throws IOException {
        Path token = token("dev-token", TOKEN + "\n");

        RuntimeFile file = read("""
                version: 1
                instance-id: laptop
                nodes:
                  dev:
                    url: http://127.0.0.1:17070
                    token-file: %s
                """.formatted(token));

        assertThat(file.instanceId().orElseThrow(), is("laptop"));
        SmolvmNode node = file.section(nodes).get("dev");
        assertThat(node.name(), is("node:dev"));
        assertThat(node.baseUrl(), is(URI.create("http://127.0.0.1:17070")));
        assertThat(node.token(), is(NodeToken.file(token)));
        assertThat(node.token().read(), is(TOKEN));
        assertThat(node.kind(), is(SmolvmNode.Kind.DECLARED));
        assertThat(node.endpoints(), is(List.of()));
    }

    @Test
    void readsANodeWithATokenVariable() throws IOException {
        environment.put("DEV_NODE_TOKEN", TOKEN);

        RuntimeFile file = read("""
                version: 1
                nodes:
                  dev:
                    url: https://node.test:7443/
                    token-env: DEV_NODE_TOKEN
                """);

        SmolvmNode node = file.section(nodes).get("dev");
        assertThat(node.baseUrl(), is(URI.create("https://node.test:7443")));
        assertThat(node.token().read(), is(TOKEN));
        assertThat(node.token().toString(), not(containsString(TOKEN)));
    }

    @Test
    void readsJsonWhenTheNameEndsInJson() throws IOException {
        environment.put("DEV_NODE_TOKEN", TOKEN);
        Path json = dir.resolve("runtime.json");
        Files.writeString(json, """
                {"version": 1, "nodes": {"dev": {"url": "http://127.0.0.1:17070", "token-env": "DEV_NODE_TOKEN"}}}
                """);

        RuntimeFile file = RuntimeFile.read(json, List.of(nodes));

        assertThat(file.section(nodes).keySet(), contains("dev"));
    }

    @Test
    void keepsTheNodesInFileOrder() throws IOException {
        environment.put("T", TOKEN);

        RuntimeFile file = read("""
                version: 1
                nodes:
                  zeta: {url: "http://127.0.0.1:1", token-env: T}
                  alpha: {url: "http://127.0.0.1:2", token-env: T}
                  mid: {url: "http://127.0.0.1:3", token-env: T}
                """);

        assertThat(file.section(nodes).keySet(), contains("zeta", "alpha", "mid"));
    }

    @Test
    void aMissingOrEmptySectionHasNoDefinitions() throws IOException {
        assertThat(read("version: 1\n").section(nodes), is(anEmptyMap()));
        assertThat(read("version: 1\nnodes:\n").section(nodes), is(anEmptyMap()));
        assertThat(read("version: 1\nnodes: {}\n").section(nodes), is(anEmptyMap()));
        assertThat(read("version: 1\n").instanceId().isPresent(), is(false));
    }

    @Test
    void theSameFileGivesEqualDefinitions() throws IOException {
        Path token = token("dev-token", TOKEN);
        String content = """
                version: 1
                nodes:
                  dev:
                    url: http://127.0.0.1:17070
                    token-file: %s
                """.formatted(token);

        assertThat(read(content), equalTo(read(content)));
        assertThat(read(content), not(equalTo(read(content.replace("17070", "17071")))));
    }

    @Test
    void theVersionIsRequired() {
        assertProblem("instance-id: laptop\n", "version: is required; this Floci reads version 1");
    }

    @Test
    void onlyVersionOneIsRead() {
        assertProblem("version: 2\n", "version: 2 is not supported; this Floci reads version 1");
        assertProblem("version: \"1\"\n", "version: must be the number 1");
        assertProblem("version: 1.5\n", "version: must be the number 1");
    }

    @Test
    void theVersionIsCheckedBeforeTheFields() {
        assertProblem("version: 2\ngateway: {}\n", "version: 2 is not supported; this Floci reads version 1");
        assertProblem("gateway: {}\n", "version: is required; this Floci reads version 1");
    }

    @Test
    void anEmptyFileIsRefused() {
        assertProblem("", "the file is empty; it needs version: 1");
        assertProblem("# nothing here\n", "the file is empty; it needs version: 1");
    }

    @Test
    void theFileMustHoldAMapping() {
        assertProblem("- 1\n", "the file must hold a mapping of version, instance-id, nodes, network, workloads");
    }

    @Test
    void aMissingFileIsRefused() {
        RuntimeFileException e = assertThrows(RuntimeFileException.class,
                () -> RuntimeFile.read(dir.resolve("absent.yaml"), List.of(nodes)));

        assertThat(e.getMessage(), is("the file does not exist"));
    }

    @Test
    void unknownTopLevelFieldsAreRefused() {
        assertProblem("version: 1\ngateway: {}\n",
                "gateway: unknown field; a runtime file has version, instance-id, nodes, network, workloads");
    }

    @Test
    void duplicateKeysAreRefused() {
        assertProblem("version: 1\nversion: 1\n", "version: duplicate key");
        assertProblem("""
                version: 1
                nodes:
                  dev: {url: "http://127.0.0.1:1", token-env: T}
                  dev: {url: "http://127.0.0.1:2", token-env: T}
                """, "nodes.dev: duplicate key");
    }

    @Test
    void duplicateKeysAreRefusedInJson() throws IOException {
        Path json = dir.resolve("runtime.json");
        Files.writeString(json, "{\"version\": 1, \"nodes\": {\"dev\": {}, \"dev\": {}}}");

        RuntimeFileException e = assertThrows(RuntimeFileException.class, () -> RuntimeFile.read(json, List.of(nodes)));

        assertThat(e.getMessage(), is("nodes.dev: duplicate key"));
    }

    @Test
    void moreThanOneDocumentIsRefused() {
        assertProblem("version: 1\n---\nversion: 1\n", "line 3, column 1: the file holds more than one document");
    }

    @Test
    void yamlAliasesAreRefused() {
        assertProblem("""
                version: 1
                instance-id: &id laptop
                nodes:
                  dev:
                    url: *id
                """, "nodes.dev.url: YAML aliases are not supported");
    }

    @Test
    void syntaxErrorsNameTheLine() {
        RuntimeFileException e = assertThrows(RuntimeFileException.class,
                () -> read("version: 1\nnodes: [dev\n"));

        assertThat(e.getMessage(), startsWith("line 3, column 1: expected ',' or ']'"));
    }

    @Test
    void theInstanceIdIsShort() {
        assertProblem("version: 1\ninstance-id: Laptop\n", "instance-id: must be 1 to 32 of a-z, 0-9 and -");
        assertProblem("version: 1\ninstance-id: " + "a".repeat(33) + "\n",
                "instance-id: must be 1 to 32 of a-z, 0-9 and -");
        assertProblem("version: 1\ninstance-id: 7\n", "instance-id: must be a nonempty string");
    }

    @Test
    void aSectionWithoutAReaderIsRefused() {
        assertProblem("version: 1\nnetwork:\n  isolation: {enabled: true}\n",
                "network: this version of Floci does not support this section");
    }

    @Test
    void aRegisteredSectionIsReadAndCheckedAgainstTheOthers() throws IOException {
        environment.put("T", TOKEN);
        RuntimeFileSection<String> workloads = new RuntimeFileSection<>() {
            @Override
            public String name() {
                return "workloads";
            }

            @Override
            public String empty() {
                return "";
            }

            @Override
            public String read(JsonNode node, Path file) {
                return RuntimeFileFields.text(node, "workloads", "node");
            }

            @Override
            public void check(String node, RuntimeFile file) {
                if (!node.isEmpty() && !file.section(nodes).containsKey(node)) {
                    throw new RuntimeFileException("workloads.node", "names no node in nodes");
                }
            }
        };
        String content = """
                version: 1
                nodes:
                  dev: {url: "http://127.0.0.1:1", token-env: T}
                workloads:
                  node: %s
                """;

        RuntimeFile file = RuntimeFile.read(write(content.formatted("dev")), List.of(nodes, workloads));
        assertThat(file.section(workloads), is("dev"));
        assertThat(RuntimeFile.read(write("version: 1\n"), List.of(nodes, workloads)).section(workloads), is(""));
        RuntimeFileException e = assertThrows(RuntimeFileException.class,
                () -> RuntimeFile.read(write(content.formatted("prod")), List.of(nodes, workloads)));
        assertThat(e.getMessage(), is("workloads.node: names no node in nodes"));
        assertThat(e.path(), is("workloads.node"));
    }

    @Test
    void aNodeNameIsShort() {
        environment.put("T", TOKEN);
        assertProblem("version: 1\nnodes:\n  dev.one: {url: \"http://127.0.0.1:1\", token-env: T}\n",
                "nodes.dev.one: a node name is 1 to 63 of A-Z, a-z, 0-9, _ and -");
        assertProblem("version: 1\nnodes:\n  " + "n".repeat(64) + ": {url: \"http://127.0.0.1:1\", token-env: T}\n",
                "nodes." + "n".repeat(64) + ": a node name is 1 to 63 of A-Z, a-z, 0-9, _ and -");
    }

    @Test
    void nodesMustBeAMappingOfMappings() {
        assertProblem("version: 1\nnodes: [dev]\n", "nodes: must be a mapping");
        assertProblem("version: 1\nnodes:\n  dev: http://127.0.0.1:1\n", "nodes.dev: must be a mapping");
    }

    @Test
    void aNodeHasOnlyUrlAndOneToken() {
        environment.put("T", TOKEN);
        assertProblem("version: 1\nnodes:\n  dev: {url: \"http://127.0.0.1:1\", token-env: T, port: 1}\n",
                "nodes.dev.port: unknown field; nodes.dev has url, token-file, token-env");
        assertProblem("version: 1\nnodes:\n  dev: {token-env: T}\n", "nodes.dev.url: is required");
        assertProblem("version: 1\nnodes:\n  dev: {url: \"http://127.0.0.1:1\"}\n",
                "nodes.dev: needs exactly one of token-file and token-env");
        assertProblem("version: 1\nnodes:\n  dev: {url: \"http://127.0.0.1:1\", token-env: T, token-file: /t}\n",
                "nodes.dev: needs exactly one of token-file and token-env");
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "ftp://127.0.0.1:21", "http://127.0.0.1", "http://user@127.0.0.1:1", "http://127.0.0.1:1/api",
        "http://127.0.0.1:1?a=b", "http://127.0.0.1:1#top", "127.0.0.1:1", "http://127.0.0.1:99999", "http:///x",
        "http://node test:1",
    })
    void aNodeUrlIsSchemeHostAndPortOnly(String url) {
        environment.put("T", TOKEN);
        assertProblem("version: 1\nnodes:\n  dev: {url: \"" + url + "\", token-env: T}\n",
                "nodes.dev.url: must be http:// or https:// with a host and a port, and nothing else");
    }

    @Test
    void aNodeUrlMayNameAnIpv6Host() throws IOException {
        environment.put("T", TOKEN);

        RuntimeFile file = read("version: 1\nnodes:\n  dev: {url: \"HTTP://[::1]:7070\", token-env: T}\n");

        assertThat(file.section(nodes).get("dev").baseUrl(), is(URI.create("http://[::1]:7070")));
    }

    @Test
    void aTokenFileIsPrivate() throws IOException {
        Path token = token("dev-token", TOKEN);
        Files.setPosixFilePermissions(token, PosixFilePermissions.fromString("rw-r-----"));

        assertProblem(tokenFileNode(token), "nodes.dev.token-file: the token file " + token
                + " is open to its group or others; only its owner may read it (chmod 600)");
    }

    @Test
    void aTokenFileHoldsOneLongVisibleToken() throws IOException {
        String rule = " must hold one token of at least 16 visible ASCII characters";
        Path shortToken = token("short", "0123456789abcde");
        assertProblem(tokenFileNode(shortToken), "nodes.dev.token-file: the token file " + shortToken + rule);
        Path twoWords = token("two", "0123456789 abcdef0123");
        assertProblem(tokenFileNode(twoWords), "nodes.dev.token-file: the token file " + twoWords + rule);
        Path notAscii = token("latin", "0123456789abcdefé");
        assertProblem(tokenFileNode(notAscii), "nodes.dev.token-file: the token file " + notAscii + rule);
        Path empty = token("empty", "");
        assertProblem(tokenFileNode(empty), "nodes.dev.token-file: the token file " + empty + rule);
    }

    @Test
    void aMissingTokenFileIsRefused() {
        Path absent = dir.resolve("absent-token");

        assertProblem(tokenFileNode(absent), "nodes.dev.token-file: the token file " + absent + " does not exist");
    }

    @Test
    void aRelativeTokenFileIsBesideTheRuntimeFile() throws IOException {
        token("dev-token", TOKEN);

        RuntimeFile file = read("""
                version: 1
                nodes:
                  dev: {url: "http://127.0.0.1:1", token-file: dev-token}
                """);

        assertThat(file.section(nodes).get("dev").token(), is(NodeToken.file(dir.resolve("dev-token"))));
    }

    @Test
    void aTokenVariableIsSetAndValid() {
        assertProblem("version: 1\nnodes:\n  dev: {url: \"http://127.0.0.1:1\", token-env: DEV_TOKEN}\n",
                "nodes.dev.token-env: the environment variable DEV_TOKEN is not set");
        environment.put("DEV_TOKEN", "short");
        assertProblem("version: 1\nnodes:\n  dev: {url: \"http://127.0.0.1:1\", token-env: DEV_TOKEN}\n",
                "nodes.dev.token-env: the environment variable DEV_TOKEN must hold one token of at least 16 "
                        + "visible ASCII characters");
    }

    @Test
    void aNodeTokenNeverShowsInItsDescription() throws IOException {
        Path token = token("dev-token", TOKEN);

        assertThat(NodeToken.file(token).toString(), not(containsString(TOKEN)));
        assertThat(NodeToken.variable("PATH").toString(), is("TokenVariable[name=PATH]"));
        assertThat(NodeToken.variable("PATH"), is(NodeToken.variable("PATH")));
        assertThat(NodeToken.file(token), instanceOf(NodeToken.TokenFile.class));
    }

    private String tokenFileNode(Path token) {
        return "version: 1\nnodes:\n  dev: {url: \"http://127.0.0.1:1\", token-file: \"" + token + "\"}\n";
    }

    private void assertProblem(String content, String message) {
        RuntimeFileException e = assertThrows(RuntimeFileException.class, () -> read(content));
        assertThat(e.getMessage(), is(message));
    }

    private RuntimeFile read(String content) throws IOException {
        return RuntimeFile.read(write(content), List.of(nodes));
    }

    private Path write(String content) throws IOException {
        Path file = dir.resolve("runtime.yaml");
        Files.writeString(file, content);
        return file;
    }

    private Path token(String name, String content) throws IOException {
        Path token = dir.resolve(name);
        Files.writeString(token, content);
        Files.setPosixFilePermissions(token, PosixFilePermissions.fromString("rw-------"));
        return token;
    }
}
