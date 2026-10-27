package io.github.hectorvent.floci.services.elasticache.proxy;

import io.github.hectorvent.floci.core.common.AwsPartitions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ElastiCacheProxyManagerTest {

    @Test
    void autoTlsSuffixesAreTheElastiCacheDomainOfEveryPartition() {
        List<String> suffixes = ElastiCacheProxyManager.tlsSuffixes(List.of("auto"));

        assertTrue(suffixes.containsAll(List.of("cache.amazonaws.com", "cache.amazonaws.com.cn",
                "cache.amazonaws.eu", "cache.c2s.ic.gov")), suffixes.toString());
        assertEquals(AwsPartitions.all().stream().map(partition -> "cache." + partition.dnsSuffix())
                .distinct().toList(), suffixes);
    }

    @Test
    void explicitTlsSuffixesReplaceTheAutomaticList() {
        assertEquals(List.of("cache.example.test"),
                ElastiCacheProxyManager.tlsSuffixes(List.of("cache.example.test", "cache.example.test")));
    }
}
