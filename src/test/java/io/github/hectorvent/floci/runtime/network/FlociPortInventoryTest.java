package io.github.hectorvent.floci.runtime.network;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

class FlociPortInventoryTest {

    @Test
    void everyPortSettingIsIncludedOrExcludedWithAReason() throws IOException {
        List<String> keys = new ArrayList<>();
        try (InputStream yaml = Files.newInputStream(Path.of("src/main/resources/application.yml"))) {
            JsonNode root = new ObjectMapper(new YAMLFactory()).readTree(yaml);
            collect(root.path("floci"), "", keys);
        }
        Set<String> known = new HashSet<>(FlociPortInventory.INCLUDED);
        known.addAll(FlociPortInventory.EXCLUDED.keySet());
        List<String> unclassified = keys.stream().filter(key -> !known.contains(key)).toList();
        assertThat("each application.yml key holding a port is in FlociPortInventory.INCLUDED or EXCLUDED",
                unclassified, is(empty()));
        for (String key : FlociPortInventory.INCLUDED) {
            assertThat(key + " is not a setting", keys.contains(key), is(true));
        }
    }

    @Test
    void extraPortsJoinTheSetAndAPortNamesItsSetting() {
        FlociPortInventory inventory = new FlociPortInventory(List.of(
                new FlociPortInventory.PortRange(4566, 4566, "port"),
                new FlociPortInventory.PortRange(5672, 5699, "services.amazonmq.amqp-host-port-base")));
        List<String> wire = inventory.ports(List.of(9000)).stream().map(FlociPortInventory.PortRange::wire).toList();
        assertThat(wire, is(List.of("4566", "5672-5699", "9000")));
        assertThat(inventory.owner(5680).orElseThrow(), is("services.amazonmq.amqp-host-port-base"));
        assertThat(inventory.owner(8080).isEmpty(), is(true));
    }

    private static void collect(JsonNode node, String path, List<String> keys) {
        for (Map.Entry<String, JsonNode> field : node.properties()) {
            String key = path.isEmpty() ? field.getKey() : path + "." + field.getKey();
            if (field.getValue().isObject()) {
                collect(field.getValue(), key, keys);
            } else if (field.getKey().contains("port") || key.equals("runtime.dns-listen")) {
                keys.add(key);
            }
        }
    }
}
