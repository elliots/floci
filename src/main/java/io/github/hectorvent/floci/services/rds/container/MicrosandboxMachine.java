package io.github.hectorvent.floci.services.rds.container;

import io.github.hectorvent.floci.services.rds.model.DatabaseEngine;
import io.quarkus.runtime.annotations.RegisterForReflection;

import java.util.ArrayList;
import java.util.List;

/** Durable transition journal. VM generations and memory checkpoints are not AWS snapshots. */
@RegisterForReflection
public class MicrosandboxMachine {
    public String id;
    public String account;
    public String runtimeId;
    public String instanceId;
    public String storageName;
    public String username;
    // Journals from the original PostgreSQL provider have neither of these fields.
    public DatabaseEngine engine = DatabaseEngine.POSTGRES;
    public List<String> serverArguments = new ArrayList<>();
    public String host;
    public int port;
    public int memoryMib;
    public int rootDiskMib = 4096;
    public String vm;
    public String phase;
    public String snapshot;
    public int generation;
    public List<String> retiredVms = new ArrayList<>();
    public List<String> retiredSnapshots = new ArrayList<>();
}
