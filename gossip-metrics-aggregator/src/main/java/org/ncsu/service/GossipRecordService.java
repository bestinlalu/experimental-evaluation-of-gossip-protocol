package org.ncsu.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.ncsu.entity.GossipDigest;
import org.ncsu.entity.GossipEvent;
import org.ncsu.entity.GossipRecord;
import org.ncsu.respository.GossipRecordRepository;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class GossipRecordService {

    @Inject
    GossipRecordRepository gossipRecordRepository;

    private static final Logger LOG = Logger.getLogger(GossipRecordService.class);

    public Boolean saveGossipRecord(List<GossipEvent> gossipEvents) {

        try {
            List<GossipRecord> gossipRecords = new ArrayList<>();

            for (GossipEvent gossipEvent : gossipEvents) {
                GossipDigest gossipDigest = gossipEvent.getGossipDigest();
                String creatorAddress = gossipEvent.getCreatorAddress();
                String forwarderAddress = gossipEvent.getForwarderAddress();
                gossipRecords.add(new GossipRecord(creatorAddress, forwarderAddress, gossipDigest));
            }

            return gossipRecordRepository.save(gossipRecords);
        } catch (Exception e) {
            LOG.error(e);
            return false;
        }
    }
}
