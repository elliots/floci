package io.github.hectorvent.floci.services.rds;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.services.rds.proxy.RdsProxyTlsCertificates;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

/**
 * REST endpoint for the RDS auth proxy's TLS certificate, for tooling that has to configure a client (a Java trust store)
 * and cannot reach floci's data directory.
 *
 * <p>{@code GET /_aws/rds/tls/ca} answers the certificate as PEM. {@code POST /_aws/rds/tls/hosts} with
 * {@code {"hosts": ["name", ...]}} makes the certificate cover those names, as it does for the hosts floci advertises: an
 * application that connects by its production host name (and verifies it, as Aurora clients do) needs that name covered.
 */
@Path("/_aws/rds/tls")
public class RdsTlsInspectionController {

    private final RdsProxyTlsCertificates certificates;
    private final ObjectMapper objectMapper;

    @Inject
    public RdsTlsInspectionController(RdsProxyTlsCertificates certificates, ObjectMapper objectMapper) {
        this.certificates = certificates;
        this.objectMapper = objectMapper;
    }

    @GET
    @Path("/ca")
    @Produces("application/x-pem-file")
    public Response ca() {
        return Response.ok(certificates.certificatePem()).build();
    }

    @POST
    @Path("/hosts")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response hosts(String body) {
        JsonNode hosts;
        try {
            hosts = objectMapper.readTree(body).path("hosts");
        } catch (Exception e) {
            return Response.status(400)
                    .entity(objectMapper.createObjectNode().put("message", "the body must be JSON: " + e.getMessage()))
                    .build();
        }
        if (!hosts.isArray()) {
            return Response.status(400)
                    .entity(objectMapper.createObjectNode().put("message", "\"hosts\" must be a list of names"))
                    .build();
        }
        for (JsonNode host : hosts) {
            certificates.ensureHost(host.asText());
        }
        return Response.noContent().build();
    }
}
