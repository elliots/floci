package io.github.hectorvent.floci.runtime.node;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;
import org.yaml.snakeyaml.error.MarkedYAMLException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * The definitions of a valid runtime file: its {@code instance-id} and the definitions of each
 * registered section. The file is YAML, or JSON when its name ends in {@code .json}, read as one
 * Jackson tree, so nothing in it needs reflection.
 */
public final class RuntimeFile {

    public static final int VERSION = 1;

    /** The sections a runtime file may have, in the order the documentation gives them. */
    public static final List<String> SECTIONS = List.of("nodes", "network", "workloads");

    private static final List<String> TOP_LEVEL = List.of("version", "instance-id", "nodes", "network", "workloads");
    private static final Pattern INSTANCE_ID = Pattern.compile("[a-z0-9-]{1,32}");

    private static final YAMLFactory YAML = YAMLFactory.builder()
            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
            .enable(YAMLParser.Feature.EMPTY_STRING_AS_NULL)
            .build();
    private static final JsonFactory JSON = JsonFactory.builder()
            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
            .build();
    private static final ObjectReader YAML_READER = reader(YAML);
    private static final ObjectReader JSON_READER = reader(JSON);

    private final String instanceId;
    private final Map<String, Object> sections;

    private RuntimeFile(String instanceId, Map<String, Object> sections) {
        this.instanceId = instanceId;
        this.sections = sections;
    }

    /** The {@code instance-id}, or empty when the file has none. */
    public Optional<String> instanceId() {
        return Optional.ofNullable(instanceId);
    }

    /** The definitions of {@code section}, or its empty definitions when the file has none. */
    public <T> T section(RuntimeFileSection<T> section) {
        Object definitions = sections.get(section.name());
        if (definitions == null) {
            throw new IllegalArgumentException("the runtime file section " + section.name() + " is not registered");
        }
        @SuppressWarnings("unchecked")
        T typed = (T) definitions;
        return typed;
    }

    /** The definitions in force when no runtime file is configured. */
    static RuntimeFile empty(Collection<? extends RuntimeFileSection<?>> sections) {
        Map<String, Object> definitions = new LinkedHashMap<>();
        for (RuntimeFileSection<?> section : sections) {
            definitions.put(section.name(), section.empty());
        }
        return new RuntimeFile(null, Map.copyOf(definitions));
    }

    /**
     * Reads and validates {@code file} with the given sections.
     *
     * @throws RuntimeFileException for the first problem
     */
    public static RuntimeFile read(Path file, Collection<? extends RuntimeFileSection<?>> sections) {
        byte[] content;
        try {
            content = Files.readAllBytes(file);
        } catch (NoSuchFileException e) {
            throw new RuntimeFileException("", "the file does not exist");
        } catch (IOException e) {
            throw new RuntimeFileException("", "the file cannot be read: " + e.getMessage());
        }
        return of(parse(file, content), file, sections);
    }

    /**
     * Validates the tree of {@code file}.
     *
     * @throws RuntimeFileException for the first problem
     */
    private static RuntimeFile of(JsonNode root, Path file, Collection<? extends RuntimeFileSection<?>> sections) {
        if (root.isMissingNode() || root.isNull()) {
            throw new RuntimeFileException("", "the file is empty; it needs version: " + VERSION);
        }
        if (!root.isObject()) {
            throw new RuntimeFileException("", "the file must hold a mapping of " + String.join(", ", TOP_LEVEL));
        }
        // The version comes first: a file of another version may have fields that this one does not know.
        JsonNode version = root.get("version");
        if (version == null) {
            throw new RuntimeFileException("version", "is required; this Floci reads version " + VERSION);
        }
        if (!version.isIntegralNumber()) {
            throw new RuntimeFileException("version", "must be the number " + VERSION);
        }
        if (!version.canConvertToInt() || version.intValue() != VERSION) {
            throw new RuntimeFileException("version",
                    version.asText() + " is not supported; this Floci reads version " + VERSION);
        }
        RuntimeFileFields.mapping(root, "", TOP_LEVEL);
        String instanceId = RuntimeFileFields.optionalText(root, "", "instance-id");
        if (instanceId != null && !INSTANCE_ID.matcher(instanceId).matches()) {
            throw new RuntimeFileException("instance-id", "must be 1 to 32 of a-z, 0-9 and -");
        }
        Map<String, RuntimeFileSection<?>> registered = new LinkedHashMap<>();
        for (RuntimeFileSection<?> section : sections) {
            registered.put(section.name(), section);
        }
        for (String name : SECTIONS) {
            JsonNode node = root.get(name);
            if (node != null && !node.isNull() && !registered.containsKey(name)) {
                throw new RuntimeFileException(name, "this version of Floci does not support this section");
            }
        }
        Map<String, Object> definitions = new LinkedHashMap<>();
        for (RuntimeFileSection<?> section : registered.values()) {
            JsonNode node = root.get(section.name());
            definitions.put(section.name(), node == null || node.isNull() ? section.empty() : section.read(node, file));
        }
        RuntimeFile runtimeFile = new RuntimeFile(instanceId, Map.copyOf(definitions));
        for (RuntimeFileSection<?> section : registered.values()) {
            check(section, runtimeFile);
        }
        return runtimeFile;
    }

    private static <T> void check(RuntimeFileSection<T> section, RuntimeFile file) {
        section.check(file.section(section), file);
    }

    private static JsonNode parse(Path file, byte[] content) {
        boolean json = file.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".json");
        try {
            if (!json) {
                refuseAliases(content);
            }
            return (json ? JSON_READER : YAML_READER).readTree(content);
        } catch (MismatchedInputException e) {
            if (e.getOriginalMessage().startsWith("Trailing token")) {
                throw new RuntimeFileException("", at(e.getLocation()) + "the file holds more than one document");
            }
            throw new RuntimeFileException("", at(e.getLocation()) + e.getOriginalMessage());
        } catch (JsonProcessingException e) {
            if (e.getCause() instanceof MarkedYAMLException yaml && yaml.getProblemMark() != null) {
                throw new RuntimeFileException("", "line " + (yaml.getProblemMark().getLine() + 1) + ", column "
                        + (yaml.getProblemMark().getColumn() + 1) + ": " + yaml.getProblem());
            }
            if (e.getOriginalMessage().startsWith("Duplicate field") && e.getProcessor() instanceof JsonParser parser) {
                throw new RuntimeFileException(path(parser.getParsingContext().pathAsPointer()), "duplicate key");
            }
            throw new RuntimeFileException("", at(e.getLocation()) + e.getOriginalMessage());
        } catch (IOException e) {
            throw new RuntimeFileException("", "the file cannot be read: " + e.getMessage());
        }
    }

    /** Jackson reads a YAML alias as its anchor's name, so the tree would hold the wrong value. */
    private static void refuseAliases(byte[] content) throws IOException {
        try (JsonParser parser = YAML.createParser(content)) {
            while (parser.nextToken() != null) {
                if (parser instanceof YAMLParser yaml && yaml.isCurrentAlias()) {
                    throw new RuntimeFileException(path(parser.getParsingContext().pathAsPointer()),
                            "YAML aliases are not supported");
                }
            }
        }
    }

    private static String path(JsonPointer pointer) {
        StringBuilder path = new StringBuilder();
        for (JsonPointer segment = pointer; segment != null && !segment.matches(); segment = segment.tail()) {
            if (!path.isEmpty()) {
                path.append('.');
            }
            path.append(segment.getMatchingProperty());
        }
        return path.toString();
    }

    private static String at(JsonLocation location) {
        if (location == null || location.getLineNr() < 1) {
            return "";
        }
        return "line " + location.getLineNr() + ", column " + location.getColumnNr() + ": ";
    }

    private static ObjectReader reader(JsonFactory factory) {
        return new ObjectMapper(factory).reader().with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof RuntimeFile file
                && Objects.equals(instanceId, file.instanceId)
                && sections.equals(file.sections);
    }

    @Override
    public int hashCode() {
        return Objects.hash(instanceId, sections);
    }
}
