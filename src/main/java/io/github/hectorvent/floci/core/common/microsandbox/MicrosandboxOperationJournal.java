package io.github.hectorvent.floci.core.common.microsandbox;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.Resettable;
import io.github.hectorvent.floci.core.storage.AccountAwareStorageBackend;
import io.github.hectorvent.floci.core.storage.StorageFactory;
import io.quarkus.runtime.annotations.RegisterForReflection;
import jakarta.enterprise.context.ApplicationScoped;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** Records accepted helper mutations until their actual completion response is known. */
@ApplicationScoped
public class MicrosandboxOperationJournal implements Resettable {
    private final AccountAwareStorageBackend<PendingCommand> journal;
    private final String account;
    private final Set<String> active = new HashSet<>();

    public MicrosandboxOperationJournal(StorageFactory storageFactory, EmulatorConfig config) {
        account = config.defaultAccountId();
        journal = storageFactory.create("microsandbox", "microsandbox-commands.json",
                new TypeReference<Map<String, PendingCommand>>() {});
    }

    synchronized String begin(String helper) {
        requireRecovered(helper);
        String id = UUID.randomUUID().toString();
        journal.putForAccount(account, id, new PendingCommand(id, identity(helper)));
        journal.flush();
        active.add(id);
        return id;
    }

    synchronized void complete(String id) {
        journal.deleteForAccount(account, id);
        journal.flush();
        active.remove(id);
    }

    synchronized void lost(String id) {
        active.remove(id);
    }

    synchronized void requireRecovered(String helper) {
        String identity = identity(helper);
        if (journal.scanAllAccounts().stream().anyMatch(command -> command.helper().equals(identity)
                && !active.contains(command.id()))) {
            throw pending();
        }
    }

    @Override
    public synchronized void checkReset() {
        if (journal.scanAllAccounts().stream().anyMatch(command -> !active.contains(command.id()))) {
            throw pending();
        }
    }

    @Override
    public synchronized void beforeReset() {
        if (!journal.scanAllAccounts().isEmpty()) {
            throw pending();
        }
    }

    @Override
    public void clear() {
        // beforeReset refuses to erase evidence of an unconfirmed command.
    }

    private static IllegalStateException pending() {
        return new IllegalStateException("Unconfirmed microsandbox commands prevent mutation or reset; "
                + "stop the native helper and reconcile its guests before removing recovery records "
                + "from microsandbox-commands.json");
    }

    private static String identity(String helper) {
        return UUID.nameUUIDFromBytes(helper.getBytes(StandardCharsets.UTF_8)).toString();
    }

    @RegisterForReflection
    public record PendingCommand(String id, String helper) {}
}
