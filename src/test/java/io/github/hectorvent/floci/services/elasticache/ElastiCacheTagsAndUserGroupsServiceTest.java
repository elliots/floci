package io.github.hectorvent.floci.services.elasticache;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.RegionResolver;
import io.github.hectorvent.floci.core.common.ServiceConfigAccess;
import io.github.hectorvent.floci.core.storage.StorageFactory;
import io.github.hectorvent.floci.services.ec2.Ec2Service;
import io.github.hectorvent.floci.services.elasticache.container.ElastiCacheContainerManager;
import io.github.hectorvent.floci.services.elasticache.container.ValkeyClusterFormation;
import io.github.hectorvent.floci.services.elasticache.model.AuthMode;
import io.github.hectorvent.floci.services.elasticache.proxy.ElastiCacheProxyManager;
import io.github.hectorvent.floci.services.kms.KmsService;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ElastiCacheTagsAndUserGroupsServiceTest {

    private static final String REGION = "us-east-1";

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
                new RegionResolver(REGION, "000000000000"), mock(KmsService.class), new ElastiCacheProvisioningIds());
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
        service.createUserGroup("team", "valkey", List.of(), REGION, Map.of("initial", "yes"));
        service.addTagsToResource(arn("user:owner"), Map.of("env", "dev", "remove", "me"), REGION);
        service.addTagsToResource(arn("user:owner"), Map.of("env", "test"), REGION);
        service.removeTagsFromResource(arn("user:owner"), List.of("remove"), REGION);
        service.addTagsToResource(arn("usergroup:team"), Map.of("team", "cache"), REGION);
        first.shutdownAll();

        StorageFactory second = factory(mode);
        try {
            ElastiCacheService restored = service(second);
            assertEquals(Map.of("env", "test"), restored.getUser("owner").getTags());
            assertEquals(Map.of("initial", "yes", "team", "cache"), restored.getUserGroup("team").getTags());
            restored.deleteUserGroup("team");
            restored.deleteUser("owner");
            restored.createUser("owner", "default", AuthMode.NO_AUTH, List.of(), "on", "redis");
            assertTrue(restored.getUser("owner").getTags().isEmpty());
        } finally {
            second.shutdownAll();
        }
    }
}
