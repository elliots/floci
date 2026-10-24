package io.github.hectorvent.floci.runtime.node;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;
import java.util.Map;

/** Field checks that every section of the runtime file shares. Each names the path at fault. */
public final class RuntimeFileFields {

    private RuntimeFileFields() {
    }

    /** The path of {@code key} below {@code parent}. */
    public static String path(String parent, String key) {
        return parent.isEmpty() ? key : parent + "." + key;
    }

    /** Requires {@code node} to be a mapping whose keys are all in {@code allowed}. */
    public static void mapping(JsonNode node, String path, List<String> allowed) {
        mapping(node, path);
        for (Map.Entry<String, JsonNode> field : node.properties()) {
            String name = field.getKey();
            if (!allowed.contains(name)) {
                throw new RuntimeFileException(path(path, name), "unknown field; "
                        + (path.isEmpty() ? "a runtime file" : path) + " has " + String.join(", ", allowed));
            }
        }
    }

    /** Requires {@code node} to be a mapping. */
    public static void mapping(JsonNode node, String path) {
        if (!node.isObject()) {
            throw new RuntimeFileException(path, "must be a mapping");
        }
    }

    /** The string at {@code key} of the mapping {@code node}, or null when it has none. */
    public static String optionalText(JsonNode node, String path, String key) {
        JsonNode value = node.get(key);
        if (value == null) {
            return null;
        }
        if (!value.isTextual() || value.textValue().isBlank()) {
            throw new RuntimeFileException(path(path, key), "must be a nonempty string");
        }
        return value.textValue();
    }

    /** The string at {@code key} of the mapping {@code node}, which must have one. */
    public static String text(JsonNode node, String path, String key) {
        String value = optionalText(node, path, key);
        if (value == null) {
            throw new RuntimeFileException(path(path, key), "is required");
        }
        return value;
    }
}
