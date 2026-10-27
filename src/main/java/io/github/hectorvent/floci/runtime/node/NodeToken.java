package io.github.hectorvent.floci.runtime.node;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.EnumSet;
import java.util.Set;
import java.util.function.UnaryOperator;

/**
 * Where the bearer token of a node service's API comes from. A token is one word of at least
 * {@link #MIN_LENGTH} visible ASCII characters, the node service's own rule. Its
 * {@code toString} names where it comes from and never holds the token.
 */
public sealed interface NodeToken permits NodeToken.TokenFile, NodeToken.TokenVariable {

    int MIN_LENGTH = 16;

    /**
     * Reads the token.
     *
     * @throws IllegalStateException when it cannot be read or breaks the rules; the message
     *                               never holds the token
     */
    String read();

    /** A token in a file that neither its group nor others may read or write. */
    static NodeToken file(Path path) {
        return new TokenFile(path);
    }

    /** A token in an environment variable of Floci's process. */
    static NodeToken variable(String name) {
        return new TokenVariable(name, TokenVariable.PROCESS_ENVIRONMENT);
    }

    /** The token in {@code text}, after trimming, if it keeps the rules. */
    private static String check(String text, String where) {
        String token = text.strip();
        if (token.length() < MIN_LENGTH || !token.chars().allMatch(c -> c > ' ' && c < 0x7f)) {
            throw new IllegalStateException(where + " must hold one token of at least " + MIN_LENGTH
                    + " visible ASCII characters");
        }
        return token;
    }

    record TokenFile(Path path) implements NodeToken {

        private static final Set<PosixFilePermission> GROUP_OR_OTHERS = EnumSet.of(
                PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_WRITE, PosixFilePermission.GROUP_EXECUTE,
                PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_WRITE, PosixFilePermission.OTHERS_EXECUTE);

        @Override
        public String read() {
            String where = "the token file " + path;
            try {
                Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(path);
                if (permissions.stream().anyMatch(GROUP_OR_OTHERS::contains)) {
                    throw new IllegalStateException(where + " is open to its group or others; "
                            + "only its owner may read it (chmod 600)");
                }
                return check(new String(Files.readAllBytes(path), StandardCharsets.ISO_8859_1), where);
            } catch (NoSuchFileException e) {
                throw new IllegalStateException(where + " does not exist");
            } catch (UnsupportedOperationException e) {
                throw new IllegalStateException(where + " is on a file system without POSIX permissions");
            } catch (IOException e) {
                throw new IllegalStateException(where + " cannot be read: " + e.getMessage());
            }
        }
    }

    /**
     * A token in an environment variable, read through {@code environment}: the process
     * environment, or a stand-in in tests.
     */
    record TokenVariable(String name, UnaryOperator<String> environment) implements NodeToken {

        static final UnaryOperator<String> PROCESS_ENVIRONMENT = System::getenv;

        @Override
        public String read() {
            String value = environment.apply(name);
            if (value == null) {
                throw new IllegalStateException("the environment variable " + name + " is not set");
            }
            return check(value, "the environment variable " + name);
        }

        @Override
        public String toString() {
            return "TokenVariable[name=" + name + "]";
        }
    }
}
