package io.github.hectorvent.floci.core.common.dns;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class NodeDnsServerServiceTest {

    private static final int A = 1;
    private static final int AAAA = 28;

    private final DnsRecordSource cloudMap = name -> name.equals("orders.shop.local")
            ? Optional.of(DnsAnswer.records(List.of("10.0.3.7"), 30)) : Optional.empty();
    private final NodeDnsServer server = new NodeDnsServer(List.of("floci.example"), List.of(cloudMap));

    @Test
    void flociNamesAndRoutedNamesGetTheNodeAddress() {
        server.update(new NodeDnsServer.Policy(Set.of("api.vendor.test"), Set.of("pay.test"), true));
        for (String name : List.of("localhost.floci.io", "s3.localhost.floci.io", "host.docker.internal",
                "floci.example", "sqs.floci.example", "api.vendor.test", "eu.pay.test", "a.b.pay.test")) {
            byte[] answer = server.answer(query(name, A));
            assertThat(name, rcode(answer), is(0));
            assertThat(name, address(answer), is("169.254.53.1"));
        }
        byte[] ipv6 = server.answer(query("localhost.floci.io", AAAA));
        assertThat(rcode(ipv6), is(0));
        assertThat(count(ipv6), is(0));
    }

    @Test
    void underIsolationOtherNamesGetNxdomainAndRecordSourcesStillAnswer() {
        server.update(new NodeDnsServer.Policy(Set.of(), Set.of("pay.test"), true));
        assertThat(rcode(server.answer(query("example.com", A))), is(3));
        assertThat(rcode(server.answer(query("pay.test", A))), is(3));
        byte[] owned = server.answer(query("orders.shop.local", A));
        assertThat(address(owned), is("10.0.3.7"));
    }

    @Test
    void anAnswerIsNotAQuery() {
        byte[] answer = server.answer(query("localhost.floci.io", A));
        assertThat(server.answer(answer) == null, is(true));
        assertThat(server.answer(new byte[5]) == null, is(true));
    }

    @Test
    void serviceProxyNamesFollowRegistrationReplacementAndRemoval() {
        server.update(new NodeDnsServer.Policy(Set.of(), Set.of(), true));
        server.serviceNames("elasticache:a", Set.of("Cache.Example.test"));
        server.serviceNames("elasticache:b", Set.of("cache.example.test"));
        assertThat(address(server.answer(query("cache.example.test", A))), is(NodeDnsServer.NODE_ADDRESS));
        assertThat(count(server.answer(query("cache.example.test", AAAA))), is(0));
        server.serviceNames("elasticache:a", Set.of("replacement.example.test"));
        assertThat(address(server.answer(query("replacement.example.test", A))), is(NodeDnsServer.NODE_ADDRESS));
        server.serviceNames("elasticache:b", Set.of());
        assertThat(rcode(server.answer(query("cache.example.test", A))), is(3));
        assertThat(rcode(server.answer(query("unregistered.example.test", A))), is(3));
    }

    private static byte[] query(String name, int type) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(new byte[] {0x12, 0x34, 0x01, 0x00, 0, 1, 0, 0, 0, 0, 0, 0});
        for (String label : name.split("\\.")) {
            out.write(label.length());
            out.writeBytes(label.getBytes(StandardCharsets.US_ASCII));
        }
        out.write(0);
        out.write(type >> 8);
        out.write(type);
        out.writeBytes(new byte[] {0, 1});
        return out.toByteArray();
    }

    private static int rcode(byte[] answer) {
        return answer[3] & 0x0f;
    }

    private static int count(byte[] answer) {
        return ByteBuffer.wrap(answer).getShort(6);
    }

    /** The first A record of an answer whose question is one name. */
    private static String address(byte[] answer) {
        int end = EmbeddedDnsServer.questionEnd(answer);
        int rdata = end + 12;
        return (answer[rdata] & 0xff) + "." + (answer[rdata + 1] & 0xff) + "." + (answer[rdata + 2] & 0xff) + "."
                + (answer[rdata + 3] & 0xff);
    }
}
