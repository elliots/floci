package io.github.hectorvent.floci.services.elasticache;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.core.common.RegionResolver;
import io.github.hectorvent.floci.core.common.ServiceConfigAccess;
import io.github.hectorvent.floci.core.storage.StorageFactory;
import io.github.hectorvent.floci.services.ec2.Ec2Service;
import io.github.hectorvent.floci.services.elasticache.container.ElastiCacheContainerManager;
import io.github.hectorvent.floci.services.elasticache.container.ElastiCacheMemcachedContainerManager;
import io.github.hectorvent.floci.services.elasticache.container.ValkeyClusterFormation;
import io.github.hectorvent.floci.services.elasticache.model.AuthMode;
import io.github.hectorvent.floci.services.elasticache.model.CacheCluster;
import io.github.hectorvent.floci.services.elasticache.model.CacheParameterGroup;
import io.github.hectorvent.floci.services.elasticache.model.CacheSubnetGroup;
import io.github.hectorvent.floci.services.elasticache.model.ReplicationGroup;
import io.github.hectorvent.floci.services.elasticache.proxy.ElastiCacheProxyManager;
import io.github.hectorvent.floci.services.kms.KmsService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ElastiCacheTagsAndUserGroupsServiceTest {

    @TempDir
    Path directory;

    private StorageFactory factory(String mode) {
        EmulatorConfig config = mock(EmulatorConfig.class, RETURNS_DEEP_STUBS);
        when(config.defaultAccountId()).thenReturn("000000000000");
        when(config.storage().persistentPath()).thenReturn(directory.toString());
        when(config.storage().wal().compactionIntervalMs()).thenReturn(60000L);
        ServiceConfigAccess access = mock(ServiceConfigAccess.class);
        when(access.storageMode("elasticache")).thenReturn(mode);
        when(access.storageFlushInterval("elasticache")).thenReturn(60000L);
        return new StorageFactory(config, access);
    }

    private ElastiCacheService service(StorageFactory factory) {
        return new ElastiCacheService(mock(ElastiCacheContainerManager.class), mock(ElastiCacheProxyManager.class),
                mock(ValkeyClusterFormation.class), factory, mock(EmulatorConfig.class), mock(Ec2Service.class),
                new RegionResolver("us-east-1", "000000000000"), mock(KmsService.class), new ElastiCacheProvisioningIds());
    }

    private String arn(String resource) {
        return "arn:aws:elasticache:us-east-1:000000000000:" + resource;
    }

    @ParameterizedTest
    @ValueSource(strings = {"persistent", "hybrid", "wal"})
    void tagsSurviveStorageLifecycle(String mode) {
        StorageFactory first = factory(mode);
        ElastiCacheService service = service(first);
        service.createUser("owner", "default", AuthMode.NO_AUTH, List.of(), "on ~* +@all", "redis");
        service.addTagsToResource(arn("user:owner"), Map.of("env", "dev", "remove", "me"));
        service.addTagsToResource(arn("user:owner"), Map.of("env", "test"));
        service.removeTagsFromResource(arn("user:owner"), List.of("remove"));
        first.shutdownAll();

        StorageFactory second = factory(mode);
        try {
            ElastiCacheService restored = service(second);
            assertEquals(Map.of("env", "test"), restored.listTagsForResource(arn("user:owner")));
            restored.deleteUser("owner");
            restored.createUser("owner", "default", AuthMode.NO_AUTH, List.of(), "on", "redis");
            assertTrue(restored.listTagsForResource(arn("user:owner")).isEmpty());
        } finally {
            second.shutdownAll();
        }
    }

    @Test
    void everyStoredResourceTypeCanBeTaggedAndReloaded() {
        StorageFactory first = factory("persistent");
        ElastiCacheService service = service(first);
        ReplicationGroup replication = new ReplicationGroup();
        replication.setReplicationGroupId("rg");
        replication.setArn(arn("replicationgroup:rg"));
        first.create("elasticache", "elasticache-groups.json", new TypeReference<Map<String, ReplicationGroup>>() {})
                .put("rg", replication);
        CacheCluster cluster = new CacheCluster();
        cluster.setCacheClusterId("redis");
        cluster.setArn(arn("cluster:redis"));
        first.create("elasticache", "elasticache-redis-clusters.json", new TypeReference<Map<String, CacheCluster>>() {})
                .put("redis", cluster);
        EmulatorConfig config = mock(EmulatorConfig.class, RETURNS_DEEP_STUBS);
        when(config.hostname()).thenReturn(Optional.of("localhost"));
        when(config.services().elasticache().defaultMemcachedImage()).thenReturn("memcached:1.6");
        ElastiCacheMemcachedService memcached = new ElastiCacheMemcachedService(
                mock(ElastiCacheMemcachedContainerManager.class), first, config, new ElastiCacheProvisioningIds(),
                new RegionResolver("us-east-1", "000000000000"));
        memcached.createCacheCluster(new ElastiCacheService.CreateCacheClusterRequest("memcached", "memcached",
                null, null, 1, null, null, null, null, null, null, null, null, null, null, null, null, null,
                "us-east-1", null));
        first.create("elasticache", "elasticache-parameter-groups.json", new TypeReference<Map<String, CacheParameterGroup>>() {})
                .put("params", new CacheParameterGroup("params", "redis7", "test"));
        first.create("elasticache", "elasticache-subnet-groups.json", new TypeReference<Map<String, CacheSubnetGroup>>() {})
                .put("subnets", new CacheSubnetGroup("subnets", "test", "vpc-test", Map.of()));
        List<String> resources = List.of("replicationgroup:rg", "cluster:redis", "cluster:memcached",
                "parametergroup:params", "subnetgroup:subnets");
        for (String resource : resources) {
            assertEquals(Map.of("name", resource), service.addTagsToResource(arn(resource), Map.of("name", resource)));
        }
        first.shutdownAll();
        StorageFactory second = factory("persistent");
        try {
            ElastiCacheService restored = service(second);
            for (String resource : resources) {
                assertEquals(Map.of("name", resource), restored.listTagsForResource(arn(resource)));
                assertTrue(restored.removeTagsFromResource(arn(resource), List.of("name")).isEmpty());
            }
        } finally {
            second.shutdownAll();
        }
    }

    @Test
    void invalidResourcesReturnTypedErrorsAndTagsAreAtomic() {
        ElastiCacheService service = service(factory("memory"));
        Map<String, String> errors = Map.of("user", "UserNotFound",
                "cluster", "CacheClusterNotFound", "replicationgroup", "ReplicationGroupNotFoundFault",
                "parametergroup", "CacheParameterGroupNotFound", "subnetgroup", "CacheSubnetGroupNotFoundFault",
                "snapshot", "SnapshotNotFoundFault");
        errors.forEach((type, code) -> assertEquals(code, assertThrows(AwsException.class,
                () -> service.addTagsToResource(arn(type + ":missing"), Map.of("x", "y"))).getErrorCode()));
        for (String invalid : List.of("invalid", arn("unknown:test"), arn("user:"),
                "arn:aws:elasticache:::user:bad", arn("usergroup:bad/id"))) {
            assertEquals("InvalidARN", assertThrows(AwsException.class,
                    () -> service.listTagsForResource(invalid)).getErrorCode());
        }
        service.createUser("u", "default", AuthMode.NO_AUTH, List.of(), "on", "redis");
        service.addTagsToResource(arn("user:u"), Map.of("keep", "yes"));
        Map<String, String> tooMany = IntStream.range(0, 50).boxed().collect(Collectors.toMap(i -> "k" + i, i -> "v"));
        assertEquals("TagQuotaPerResourceExceeded", assertThrows(AwsException.class,
                () -> service.addTagsToResource(arn("user:u"), tooMany)).getErrorCode());
        assertEquals("TagNotFound", assertThrows(AwsException.class,
                () -> service.removeTagsFromResource(arn("user:u"), List.of("keep", "missing"))).getErrorCode());
        assertEquals(Map.of("keep", "yes"), service.listTagsForResource(arn("user:u")));
    }

    @Test
    void memberTagsAreIndependentAndReplicationGroupChangesPropagate() {
        StorageFactory factory = factory("persistent");
        ElastiCacheService service = service(factory);
        ReplicationGroup group = new ReplicationGroup();
        group.setReplicationGroupId("parent");
        group.setArn(arn("replicationgroup:parent"));
        group.setNumCacheClusters(2);
        factory.create("elasticache", "elasticache-groups.json", new TypeReference<Map<String, ReplicationGroup>>() {})
                .put("parent", group);
        service.addTagsToResource(arn("replicationgroup:parent"), Map.of("shared", "first"));
        service.addTagsToResource(arn("cluster:parent-001"), Map.of("member", "one"));
        assertEquals(Map.of("shared", "first"), service.listTagsForResource(arn("cluster:parent-002")));
        assertEquals(Map.of("shared", "first"), service.listTagsForResource(arn("replicationgroup:parent")));
        service.addTagsToResource(arn("replicationgroup:parent"), Map.of("shared", "second"));
        service.removeTagsFromResource(arn("replicationgroup:parent"), List.of("shared"));
        factory.shutdownAll();
        StorageFactory reloaded = factory("persistent");
        try {
            ElastiCacheService restored = service(reloaded);
            assertEquals(Map.of("member", "one"), restored.listTagsForResource(arn("cluster:parent-001")));
            assertTrue(restored.listTagsForResource(arn("cluster:parent-002")).isEmpty());
        } finally {
            reloaded.shutdownAll();
        }
    }

    @Test
    void resourceArnIdentityAndPartitionAreValidated() {
        StorageFactory factory = factory("memory");
        ElastiCacheService service = service(factory);
        CacheCluster elsewhere = new CacheCluster();
        elsewhere.setArn("arn:aws:elasticache:eu-west-1:000000000000:cluster:elsewhere");
        factory.create("elasticache", "elasticache-redis-clusters.json", new TypeReference<Map<String, CacheCluster>>() {})
                .put("elsewhere", elsewhere);
        assertEquals("CacheClusterNotFound", assertThrows(AwsException.class,
                () -> service.listTagsForResource(arn("cluster:elsewhere"))).getErrorCode());
        ElastiCacheService china = new ElastiCacheService(mock(ElastiCacheContainerManager.class),
                mock(ElastiCacheProxyManager.class), mock(ValkeyClusterFormation.class), factory,
                mock(EmulatorConfig.class), mock(Ec2Service.class), new RegionResolver("cn-north-1", "000000000000"),
                mock(KmsService.class), new ElastiCacheProvisioningIds());
        china.createUser("china-user", "default", AuthMode.NO_AUTH, List.of(), "on", "redis");
        String userArn = "arn:aws-cn:elasticache:cn-north-1:000000000000:user:china-user";
        assertEquals(Map.of("region", "china"), china.addTagsToResource(userArn, Map.of("region", "china")));
        assertEquals("InvalidARN", assertThrows(AwsException.class,
                () -> service.listTagsForResource(userArn)).getErrorCode());
    }
}
