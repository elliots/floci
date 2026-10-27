package io.github.hectorvent.floci.runtime.node;

/**
 * A runtime file that Floci cannot use. The message starts with the path of the field at fault,
 * such as {@code nodes.dev.url}, when the problem is in one.
 */
public class RuntimeFileException extends IllegalArgumentException {

    private final String path;

    public RuntimeFileException(String path, String problem) {
        super(path == null || path.isEmpty() ? problem : path + ": " + problem);
        this.path = path == null ? "" : path;
    }

    /** The path of the field at fault, or empty for the file as a whole. */
    public String path() {
        return path;
    }
}
