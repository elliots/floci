package io.github.hectorvent.floci.runtime.node;

import com.fasterxml.jackson.databind.JsonNode;

import java.nio.file.Path;

/**
 * Reads one top-level section of the runtime file. A feature registers its section by making
 * its implementation an {@code @ApplicationScoped} bean; {@link RuntimeFileWatcher} finds every
 * one. The value a section returns must be immutable and compare by value, so that a reload
 * that changes nothing tells no listener.
 *
 * @param <T> the section's definitions
 */
public interface RuntimeFileSection<T> {

    /** The section's top-level key, one of {@link RuntimeFile#SECTIONS}. */
    String name();

    /** The definitions of a file without this section. */
    T empty();

    /**
     * Validates the section and returns its definitions.
     *
     * @param node the section's tree, never missing or null
     * @param file the runtime file, against whose directory relative paths resolve
     * @throws RuntimeFileException for the first problem, naming its path from {@link #name()}
     */
    T read(JsonNode node, Path file);

    /**
     * Checks this section's definitions against the other sections of the same file, once every
     * section is read.
     *
     * @throws RuntimeFileException for the first problem
     */
    default void check(T definitions, RuntimeFile file) {
    }
}
