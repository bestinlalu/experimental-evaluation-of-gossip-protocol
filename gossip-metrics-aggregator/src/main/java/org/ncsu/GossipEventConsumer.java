package org.ncsu;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.inject.Inject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;
import org.ncsu.entity.GossipEvent;
import org.ncsu.service.GossipRecordService;

import java.util.List;

@ApplicationScoped
public class GossipEventConsumer {

    @Inject
    GossipRecordService gossipRecordService;

    private static final Logger LOG = Logger.getLogger(GossipEventConsumer.class);

    ObjectMapper mapper = new ObjectMapper();

    @Incoming("gossip")
    @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
    public void consume(ConsumerRecord<String, String> record) {
        try {
//            LOG.info("Gossip received: " + record.value());
            mapper.registerModule(new JavaTimeModule());
            List<GossipEvent> events = mapper.readValue(record.value(), new TypeReference<List<GossipEvent>>(){});
            gossipRecordService.saveGossipRecord(events);
        } catch(Exception e) {
            LOG.error(e.getMessage());
        }
    }

}
