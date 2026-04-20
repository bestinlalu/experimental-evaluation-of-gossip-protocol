package org.ncsu.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.rest.client.RestClientBuilder;
import org.jboss.logging.Logger;
import org.ncsu.entity.Action;
import org.ncsu.entity.ActionRecord;
import org.ncsu.respository.ActionRecordRepository;

import java.net.URI;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

@ApplicationScoped
public class ActionService {

    @Inject
    ActionRecordRepository actionRecordRepository;

    @Inject
    ActionRecordService actionRecordService;

    private static final Logger LOG = Logger.getLogger(ActionService.class);

    Map<String, List<ActionRecord>> activeNodes = new HashMap<>();
    Map<String, List<ActionRecord>> inactiveNodes = new HashMap<>();


    public Boolean initiateAction(List<ActionRecord> actionRecords, Action action) {

        try {

            for (ActionRecord actionRecord : actionRecords) {
                NodeManagerService nodeManagerService = RestClientBuilder.newBuilder()
                        .baseUri(URI.create("http://" + actionRecord.getHost() + ":" + actionRecord.getManagerPort()))
                        .build(NodeManagerService.class);

                LOG.info("Initiate action record: " + "http://" + actionRecord.getHost() + ":" + actionRecord.getActionPort());

                Response response = null;
                List<Response> responses  = new ArrayList<>();

                try {
                    if (action == Action.KILL) {
                        response = nodeManagerService.killNode(actionRecord.getActionPort());

                        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                            LOG.error("Error while trying to act on node: " + actionRecord.getHost() + ":" + actionRecord.getManagerPort()
                                    + ", Action port: " + actionRecord.getActionPort() + ", Status: " + response.getStatus());
                        } else {

                            List<ActionRecord> records = new ArrayList<>(activeNodes.getOrDefault("http://" + actionRecord.getHost() + ":" + actionRecord.getManagerPort(),
                                                                                                new ArrayList<>()));
                            if (!records.isEmpty()) {
                                Iterator<ActionRecord> iterator = records.iterator();
                                while (iterator.hasNext()) {
                                    ActionRecord record = iterator.next();
                                    if (Objects.equals(record.getActionPort(), actionRecord.getActionPort())) {
                                        actionRecord = record;
                                        iterator.remove();
                                    }
                                }
                            }
                            if (records.isEmpty()) {
                                activeNodes.remove("http://" + actionRecord.getHost() + ":" + actionRecord.getManagerPort());
                            } else {
                                activeNodes.put("http://" + actionRecord.getHost() + ":" + actionRecord.getManagerPort(), records);
                            }
                            if (inactiveNodes.containsKey("http://" + actionRecord.getHost() + ":" + actionRecord.getManagerPort())) {
                                List<ActionRecord> outdatedInactiverecords = new ArrayList<>(inactiveNodes.getOrDefault("http://" + actionRecord.getHost() + ":" + actionRecord.getManagerPort(),
                                        new ArrayList<>()));
                                outdatedInactiverecords.add(actionRecord);
                                inactiveNodes.put("http://" + actionRecord.getHost() + ":" + actionRecord.getManagerPort(), outdatedInactiverecords);
                            } else {
                                inactiveNodes.put("http://" + actionRecord.getHost() + ":" + actionRecord.getManagerPort(), List.of(actionRecord));
                            }
                        }

                        actionRecord = actionRecordService.createActionRecord(actionRecord, response, action);

                    } else if (action == Action.START) {
                        response = nodeManagerService.startNode(actionRecord.getHost(), actionRecord.getActionPort(), actionRecord.getPeers(),
                                actionRecord.getKafkaBroker(), actionRecord.getTopic(), actionRecord.getStrategy().toString());

                        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                            LOG.error("Error while trying to act on node: " + actionRecord.getHost() + ":" + actionRecord.getManagerPort()
                                    + ", Action port: " + actionRecord.getActionPort() + ", Status: " + response.getStatus());
                        } else {
                            if (activeNodes.containsKey("http://" + actionRecord.getHost() + ":" + actionRecord.getManagerPort())) {
                                List<ActionRecord> records = new ArrayList<>(activeNodes.getOrDefault("http://" + actionRecord.getHost() + ":" + actionRecord.getManagerPort(),
                                                                            new ArrayList<>()));
                                records.add(actionRecord);
                                activeNodes.put("http://" + actionRecord.getHost() + ":" + actionRecord.getManagerPort(), records);
                            } else {
                                activeNodes.put("http://" + actionRecord.getHost() + ":" + actionRecord.getManagerPort(), List.of(actionRecord));
                            }

                            List<ActionRecord> records = new ArrayList<>(inactiveNodes.getOrDefault("http://" + actionRecord.getHost() + ":" + actionRecord.getManagerPort(),
                                                                        new ArrayList<>()));

                            if (!records.isEmpty()) {
                                final ActionRecord targetRecord = actionRecord;
                                records.removeIf(record ->
                                        Objects.equals(record.getActionPort(), targetRecord.getActionPort())
                                );
                            }
                            if (records.isEmpty()) {
                                inactiveNodes.remove("http://" + actionRecord.getHost() + ":" + actionRecord.getManagerPort());
                            } else {
                                inactiveNodes.put("http://" + actionRecord.getHost() + ":" + actionRecord.getManagerPort(), records);
                            }
                        }

                        actionRecord = actionRecordService.createActionRecord(actionRecord, response, action);

                    }
                } catch (WebApplicationException e) {
                    LOG.error(e);
                    response = e.getResponse();
                    actionRecord.setStatus(response.getStatus());
                    actionRecord.setTimestamp(LocalDateTime.ofInstant(Instant.now(), ZoneId.systemDefault()));
                }

                boolean isSaved = actionRecordRepository.save(actionRecord);

                if (!isSaved) {
                    LOG.error("Error while trying to save action on node: " + actionRecord.getHost() + ":"
                            + actionRecord.getManagerPort() + ", Action port: " + actionRecord.getActionPort()
                            + ", Status: " + response.getStatus() + ", Action: " + actionRecord.getAction());
                    return false;
                }
                LOG.info("Action record: " + actionRecord.getTopic());
            }

            return true;
        } catch (Exception e) {
            LOG.error("Exception: " + e);
            return false;
        }
    }

    public Boolean killAllNodes() {

        try {

            if (activeNodes.isEmpty()) {
                LOG.info("No active nodes found");
            }
            for (String activeHost : activeNodes.keySet()) {
                NodeManagerService nodeManagerService = RestClientBuilder.newBuilder()
                        .baseUri(URI.create(activeHost))
                        .build(NodeManagerService.class);
                Response response = null;
                try {
                    response = nodeManagerService.killAll();
                } catch (WebApplicationException e) {
                    LOG.error(e);
                    response = e.getResponse();
                }
                if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                    LOG.error("Error while trying to Kill all on nodes on address: " + activeHost + ", Status: " + response.getStatus());
                }
                else {
                    List<ActionRecord> actionRecords = activeNodes.get(activeHost);
                    LOG.info("Action records: " + actionRecords);
                    if (inactiveNodes.containsKey(activeHost)) {
                        List<ActionRecord> records = new ArrayList<>(inactiveNodes.get(activeHost));
                        LOG.info("Inaction records: " + records);
                        records.addAll(activeNodes.get(activeHost));
                        inactiveNodes.put(activeHost, records);
                    } else {
                        LOG.info("Inaction records 22: " + activeNodes.get(activeHost));
                        inactiveNodes.put(activeHost, activeNodes.get(activeHost));
                        LOG.info("Inaction records 223: " + activeNodes.get(activeHost));
                    }
                    activeNodes.remove(activeHost);

                    if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                        LOG.error("Error while trying to act on node: " + activeHost);
                    }

                    LOG.info("Action records33: " + activeNodes);

                    for (ActionRecord actionRecord : actionRecords) {
                        LOG.info("Action record: " + actionRecord.getTopic());
                        actionRecord = actionRecordService.createActionRecord(actionRecord, response, Action.KILLALL);
                        LOG.info("Action record: " + actionRecord.getActionPort());
                        boolean isSaved = actionRecordRepository.save(actionRecord);

                        if (!isSaved) {
                            LOG.error("Error while trying to save action on node: " + actionRecord.getHost() + ":"
                                    + actionRecord.getManagerPort() + ", Action port: " + actionRecord.getActionPort()
                                    + ", Status: " + response.getStatus() + ", Action: " + actionRecord.getAction());
                        }
                    }
                }

            }
        } catch (Exception e) {
            LOG.error("Exception: " + e.toString());
            return false;
        }
        return true;
    }
}
