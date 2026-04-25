package org.ncsu;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;
import org.ncsu.entity.MetricsEvent;
import org.ncsu.service.GossipRecordService;
import org.ncsu.service.MetricsService;

import java.util.List;

@ApplicationScoped
public class MetricsEventConsumer {

    @Inject
    MetricsService metricsService;

    private static final Logger LOG = Logger.getLogger(MetricsEventConsumer.class);

    ObjectMapper mapper = new ObjectMapper();

    @Incoming("metrics")
    @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
    public void consume(ConsumerRecord<String, String> record) {
        try {
//            LOG.info("Gossip received: " + record.value());
            mapper.registerModule(new JavaTimeModule());
            MetricsEvent events = mapper.readValue(record.value(), MetricsEvent.class);
            metricsService.saveMetricsEvent(events);
        } catch(Exception e) {
            LOG.error(e.getMessage());
        }
    }

}
