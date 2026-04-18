package org.ncsu.service;

import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

import jakarta.ws.rs.Path;

import java.util.List;

@Path("/action")
@RegisterRestClient
public interface NodeManagerService {

    @POST
    @Path("/start")
    Response startNode(@HeaderParam("address") String address,
                       @HeaderParam("port") Integer actionPort,
                       @HeaderParam("peers") List<String> peers,
                       @HeaderParam("kafka-broker") String kafkaBroker,
                       @HeaderParam("kafka-topic") String kafkaTopic,
                       @HeaderParam("strategy") String strategy);

    @POST
    @Path("/kill")
    Response killNode(@HeaderParam("port") Integer actionPort);

    @POST
    @Path("/kill/all")
    Response killAll();
}