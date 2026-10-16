package io.github.hectorvent.floci.runtime.network;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.github.hectorvent.floci.config.EmulatorConfig;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Optional;

/** Binds a native helper to the same immutable environment boundary as Floci. */
public final class NativeNetworkPolicy {
    private NativeNetworkPolicy() {}

    public static String expectedFingerprint(EmulatorConfig config) {
        if (config.network() == null) {
            return "";
        }
        Optional<String> file = config.network().configFile();
        if (file.isEmpty()) {
            return "";
        }
        try (InputStream input = Files.newInputStream(Path.of(file.orElseThrow()))) {
            byte[] content = input.readNBytes(1_048_577);
            if (content.length > 1_048_576) {
                throw new IllegalArgumentException("Workload network configuration exceeds 1 MiB");
            }
            JsonNode root = new ObjectMapper(new YAMLFactory().enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION))
                    .readTree(content);
            NetworkDefinition network = NetworkConfigLoader.parse(root);
            if (!network.isolated()) {
                return "";
            }
            if (!root.path("network").path("native").isObject()) {
                throw new IllegalArgumentException("Native runtime isolation requires network.native configuration");
            }
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(content));
        } catch (IOException | NoSuchAlgorithmException e) {
            throw new IllegalArgumentException("Cannot read native network boundary", e);
        }
    }
}
