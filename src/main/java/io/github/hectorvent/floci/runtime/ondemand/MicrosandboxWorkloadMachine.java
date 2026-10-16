package io.github.hectorvent.floci.runtime.ondemand;

import io.quarkus.runtime.annotations.RegisterForReflection;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Recovery journal for an owned VM. A checkpoint is never cloned into multiple generations. */
@RegisterForReflection
public class MicrosandboxWorkloadMachine {
    public String id;
    public String account;
    public String workload;
    public String specification;
    public String host;
    public String bindHost;
    public int hostPort;
    public int guestPort;
    public int memoryMib;
    public String vm;
    public String source;
    public String checkpoint;
    public int generation;
    public String phase = "NEW";
    public List<String> retiredCheckpoints = new ArrayList<>();
    public Map<String, String> generationParents = new LinkedHashMap<>();
    public Map<String, String> checkpointIds = new LinkedHashMap<>();
}
