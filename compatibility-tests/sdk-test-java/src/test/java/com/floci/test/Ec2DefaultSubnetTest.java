package com.floci.test;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.Subnet;
import software.amazon.awssdk.services.ec2.model.Vpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("EC2 default subnets")
class Ec2DefaultSubnetTest {

    @Test
    @DisplayName("SDK creates a replacement default subnet and receives the duplicate error")
    void recreateDefaultSubnet() {
        try (Ec2Client client = TestFixtures.ec2Client("default-subnet-sdk")) {
            Vpc vpc = client.createDefaultVpc(r -> {}).vpc();
            assertThat(vpc.isDefault()).isTrue();
            Subnet prior = client.describeSubnets(r -> {}).subnets().stream()
                    .filter(s -> s.defaultForAz() && s.vpcId().equals(vpc.vpcId())).findFirst().orElseThrow();
            client.deleteSubnet(r -> r.subnetId(prior.subnetId()));
            Subnet created = client.createDefaultSubnet(r -> r.availabilityZone(prior.availabilityZone())).subnet();
            assertThat(created.defaultForAz()).isTrue();
            assertThat(created.mapPublicIpOnLaunch()).isTrue();
            assertThat(created.availableIpAddressCount()).isEqualTo(4091);
            assertThat(created.cidrBlock()).endsWith("/20");
            assertThat(created.vpcId()).isEqualTo(vpc.vpcId());
            assertThat(client.describeSubnets(r -> r.subnetIds(created.subnetId())).subnets().get(0).defaultForAz()).isTrue();
            assertThatThrownBy(() -> client.createDefaultSubnet(r -> r.availabilityZone(prior.availabilityZone())))
                    .isInstanceOfSatisfying(Ec2Exception.class, error -> assertThat(error.awsErrorDetails().errorCode())
                            .isEqualTo("DefaultSubnetAlreadyExistsInAvailabilityZone"));
        }
    }
}
