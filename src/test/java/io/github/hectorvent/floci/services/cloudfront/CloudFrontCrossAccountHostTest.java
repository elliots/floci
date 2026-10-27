package io.github.hectorvent.floci.services.cloudfront;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.core.common.RegionResolver;
import io.github.hectorvent.floci.core.common.RequestContext;
import io.github.hectorvent.floci.core.storage.AccountAwareStorageBackend;
import io.github.hectorvent.floci.core.storage.AccountAwareStorageBackend.AccountEntry;
import io.github.hectorvent.floci.core.storage.InMemoryStorage;
import io.github.hectorvent.floci.core.storage.StorageFactory;
import io.github.hectorvent.floci.services.cloudfront.model.Distribution;
import io.github.hectorvent.floci.services.cloudfront.model.DistributionConfig;
import jakarta.enterprise.inject.Instance;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

/**
 * A viewer request to a distribution's host carries no credential, so the host alone selects the
 * distribution, whichever account owns it. Alternate domain names are unique across accounts, as in AWS.
 */
class CloudFrontCrossAccountHostTest {

    private static final String DEFAULT_ACCT = "000000000000";
    private static final String ACCOUNT_A = "000000000001";
    private static final String ACCOUNT_B = "000000000002";

    private final AtomicReference<String> caller = new AtomicReference<>(DEFAULT_ACCT);

    @SuppressWarnings("unchecked")
    private CloudFrontService service() {
        RequestContext rc = Mockito.mock(RequestContext.class);
        when(rc.getAccountId()).thenAnswer(inv -> caller.get());
        Instance<RequestContext> ctx = Mockito.mock(Instance.class);
        when(ctx.get()).thenReturn(rc);

        StorageFactory storageFactory = Mockito.mock(StorageFactory.class);
        when(storageFactory.create(Mockito.anyString(), Mockito.anyString(), Mockito.any()))
                .thenAnswer(invocation -> new AccountAwareStorageBackend<>(new InMemoryStorage<>(), ctx, DEFAULT_ACCT));

        EmulatorConfig config = Mockito.mock(EmulatorConfig.class);
        EmulatorConfig.DnsConfig dnsConfig = Mockito.mock(EmulatorConfig.DnsConfig.class);
        EmulatorConfig.ServicesConfig servicesConfig = Mockito.mock(EmulatorConfig.ServicesConfig.class);
        EmulatorConfig.CloudFrontServiceConfig cloudFrontConfig = Mockito.mock(EmulatorConfig.CloudFrontServiceConfig.class);
        when(config.defaultAccountId()).thenReturn(DEFAULT_ACCT);
        when(config.hostname()).thenReturn(Optional.empty());
        when(config.effectiveBaseUrl()).thenReturn("http://localhost:4566");
        when(config.dns()).thenReturn(dnsConfig);
        when(dnsConfig.extraSuffixes()).thenReturn(Optional.empty());
        when(config.services()).thenReturn(servicesConfig);
        when(servicesConfig.cloudfront()).thenReturn(cloudFrontConfig);
        when(cloudFrontConfig.domainSuffix()).thenReturn(Optional.of("cloudfront.net"));

        return new CloudFrontService(storageFactory, config, new RegionResolver("us-east-1", DEFAULT_ACCT));
    }

    private Distribution create(CloudFrontService service, String account, String alias) {
        caller.set(account);
        DistributionConfig cfg = new DistributionConfig();
        cfg.setEnabled(true);
        cfg.setAliases(List.of(alias));
        Distribution dist = new Distribution();
        dist.setConfig(cfg);
        return service.createDistribution(dist, Map.of());
    }

    @Test
    void hostResolvesToTheDistributionOfAnyAccountAndNamesItsOwner() {
        CloudFrontService service = service();
        Distribution dist = create(service, ACCOUNT_A, "pay.example.test");

        // The viewer is no account in particular: the default one.
        caller.set(DEFAULT_ACCT);

        AccountEntry<Distribution> byAlias = service.findEntryByHost("pay.example.test");
        assertEquals(dist.getId(), byAlias.value().getId());
        assertEquals(ACCOUNT_A, byAlias.accountId());
        assertEquals(dist.getId(), service.findByHost(dist.getDomainName()).getId());
        assertNull(service.findEntryByHost("other.example.test"));
    }

    @Test
    void aliasIsUniqueAcrossAccounts() {
        CloudFrontService service = service();
        create(service, ACCOUNT_A, "pay.example.test");

        AwsException e = assertThrows(AwsException.class, () -> create(service, ACCOUNT_B, "PAY.example.test"));
        assertEquals("CNAMEAlreadyExists", e.getErrorCode());
    }
}
