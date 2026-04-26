package org.ncsu;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;
import org.ncsu.service.GossipProcessManager;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Path("/action")
@ApplicationScoped
public class NodeManagerResource {

    // Inject the process manager service we created above
    @Inject
    GossipProcessManager processManager;

    private static final Logger LOG = Logger.getLogger(NodeManagerResource.class);

    ObjectMapper mapper = new ObjectMapper();

    @POST
    @Path("/start")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response start(
            @HeaderParam("id") String id,
            @HeaderParam("address") String address,
            @HeaderParam("port") Integer port,
            @HeaderParam("peers") List<String> peers,
            @HeaderParam("kafka-topic") String kafkaTopic,
            @HeaderParam("kafka-broker") String kafkaBroker,
            @HeaderParam("strategy") String strategy
    ) throws JsonProcessingException {

        LOG.info("Received start request");
        if (port == null) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(mapper.writeValueAsString(
                            Map.of("error", "action-port header is required", "timestamp", Instant.now().toString()))
                    )
                    .build();
        }

        try {
            processManager.startNode(id, address, port, peers, kafkaTopic, kafkaBroker, strategy);

            LOG.info("Process started successfully");

            return Response.ok(
                    mapper.writeValueAsString(
                            Map.of(
                        "status", "Gossip Node Started",
                        "nodeId", "Node-" + port,
                        "timestamp", Instant.now().toString()
                        )
            )).build();

        } catch (IllegalStateException e) {
            return Response.status(Response.Status.CONFLICT)
                    .entity(mapper.writeValueAsString(
                            Map.of("error", e.getMessage(), "timestamp", Instant.now().toString()))
                    )
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        } catch (Exception e) {
            LOG.error(e.getMessage());
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(mapper.writeValueAsString(
                            Map.of("status", "Error executing process", "error", e.getMessage(), "timestamp", Instant.now().toString()))
                    )
                    .build();
        }
    }

    @POST
    @Path("/kill")
    @Produces(MediaType.APPLICATION_JSON)
    public Response kill(@HeaderParam("port") Integer port) throws JsonProcessingException {
        if (port == null) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(mapper.writeValueAsString(
                            Map.of("error", "action-port header is required", "timestamp", Instant.now().toString()))
                    )
                    .build();
        }

        boolean wasKilled = processManager.killNode(port);

        if (!wasKilled) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(mapper.writeValueAsString(
                            Map.of("status", "No active node found on port " + port, "timestamp", Instant.now().toString()))
                    )
                    .build();
        }

        return Response.ok().entity(mapper.writeValueAsString(
                Map.of(
                "status", "Gossip Node Killed",
                "nodeId", "Node-" + port,
                "timestamp", Instant.now().toString()
                )
        )).build();
    }

    @POST
    @Path("/kill/all")
    @Produces(MediaType.APPLICATION_JSON)
    public Response killAll() throws JsonProcessingException {
        boolean wasKilled = processManager.killAll();

        if (!wasKilled) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(mapper.writeValueAsString(
                            Map.of("status", "No active node found", "timestamp", Instant.now().toString()))
                    )
                    .build();
        }

        return Response.ok(
            mapper.writeValueAsString(
                    Map.of(
                            "status", "All Gossip Nodes Killed",
                            "timestamp", Instant.now().toString()
                    )
        )).build();
    }
}