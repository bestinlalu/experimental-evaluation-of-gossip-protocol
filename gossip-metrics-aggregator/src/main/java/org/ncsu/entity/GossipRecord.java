package org.ncsu.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.persistence.Entity;

import java.time.LocalDateTime;

@Entity
public class GossipRecord extends PanacheEntity {

    String creatorAddress;
    String forwarderAddress;
    Long generation;
    Integer version;
    LocalDateTime timestamp;
    String uid;
    String data;
    Integer ttl;

    public GossipRecord() {}

    public  GossipRecord(String creatorAddress, String forwarderAddress, GossipDigest gossipDigest) {
        this.creatorAddress = creatorAddress;
        this.forwarderAddress = forwarderAddress;
        this.generation = gossipDigest.getGeneration();
        this.version = gossipDigest.getVersion();
        this.timestamp = gossipDigest.getTimestamp();
        this.uid = gossipDigest.getUid();
        this.data = gossipDigest.getData();
        this.ttl = gossipDigest.getTtl();
    }
}
