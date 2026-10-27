package io.github.hectorvent.floci.services.sqs;

import java.util.Objects;

/** An SQS queue by its account, region and name. */
public record SqsQueueId(String accountId, String region, String name) {

    public SqsQueueId {
        Objects.requireNonNull(accountId, "accountId");
        Objects.requireNonNull(region, "region");
        Objects.requireNonNull(name, "name");
    }

    /** The key under which SQS keeps the queue: {@code REGION::/ACCOUNT/NAME}. */
    String storageKey() {
        return region + "::/" + accountId + "/" + name;
    }

    /** The queue of a storage key {@code REGION::/ACCOUNT/NAME}, or null for a key of another shape. */
    static SqsQueueId ofStorageKey(String storageKey) {
        if (storageKey == null) {
            return null;
        }
        int separator = storageKey.indexOf("::/");
        if (separator <= 0) {
            return null;
        }
        String path = storageKey.substring(separator + 3);
        int slash = path.indexOf('/');
        if (slash <= 0 || slash == path.length() - 1 || path.indexOf('/', slash + 1) >= 0) {
            return null;
        }
        return new SqsQueueId(path.substring(0, slash), storageKey.substring(0, separator), path.substring(slash + 1));
    }
}
