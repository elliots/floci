package io.github.hectorvent.floci.runtime.node;

import java.util.List;

/**
 * A call to a node service's API that failed: an error answer, by its code, or no answer. The
 * message never holds the bearer token.
 */
public class NodeApiException extends RuntimeException {

    /** What went wrong. The node's error codes are in tools/smolvm-pods/docs/configuration.md. */
    public enum Code {
        /** 400 {@code invalid}: a bad body, query, reason or lease. */
        INVALID("invalid"),
        /** 401 {@code unauthorized}, also after the token was read again, or no token to send. */
        UNAUTHORIZED("unauthorized"),
        /** 404 {@code not_found}: no such pod. */
        NOT_FOUND("not_found"),
        /** 404 {@code lease_not_found}: the lease to renew expired or does not exist. */
        LEASE_NOT_FOUND("lease_not_found"),
        /** 409 {@code refused}: the wake or hibernate cannot run now; {@link #reasons()} says why. */
        REFUSED("refused"),
        /** 409 {@code cancelled}: a wake cancelled the hibernate's pause. */
        CANCELLED("cancelled"),
        /** 409 {@code failed}: the pod failed. */
        FAILED("failed"),
        /** 409 {@code stale_incarnation}: the call named an older incarnation of the pod. */
        STALE_INCARNATION("stale_incarnation"),
        /** 410 {@code gone}: the pod's lifecycle ended during the call. */
        GONE("gone"),
        /** 500 {@code vm_error}. */
        VM_ERROR("vm_error"),
        /** 500 {@code store_error}. */
        STORE_ERROR("store_error"),
        /** An answer the client cannot read, or an error code it does not know. */
        UNEXPECTED(null),
        /** No answer: the connection failed, timed out or was interrupted. */
        UNREACHABLE(null);

        private final String wire;

        Code(String wire) {
            this.wire = wire;
        }

        static Code of(String wire) {
            for (Code code : values()) {
                if (code.wire != null && code.wire.equals(wire)) {
                    return code;
                }
            }
            return UNEXPECTED;
        }
    }

    private final Code code;
    private final int status;
    private final List<String> reasons;

    public NodeApiException(Code code, int status, String message, List<String> reasons) {
        super(message);
        this.code = code;
        this.status = status;
        this.reasons = List.copyOf(reasons);
    }

    public NodeApiException(Code code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
        this.status = 0;
        this.reasons = List.of();
    }

    public Code code() {
        return code;
    }

    /** The HTTP status, or 0 when there was no answer. */
    public int status() {
        return status;
    }

    /** The {@code reasons} of a {@link Code#REFUSED} answer, such as {@code state creating}. */
    public List<String> reasons() {
        return reasons;
    }
}
