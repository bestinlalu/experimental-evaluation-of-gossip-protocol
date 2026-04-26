package org.ncsu.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;

import java.time.LocalDateTime;

@Entity
public class GossipRecord extends PanacheEntity {

    String creatorAddress;
    String forwarderAddress;
    Long generation;
    Integer version;
    LocalDateTime creationTimestamp;
    LocalDateTime forwarderTimestamp;
    String uid;
    String data;
    Integer ttl;
    @Column(columnDefinition = "LONGTEXT")
    String activeNeighbors;
    @Column(columnDefinition = "LONGTEXT")
    String inactiveNeighbors;

    public GossipRecord() {}

    public  GossipRecord(String creatorAddress, String forwarderAddress, GossipDigest gossipDigest) {
        this.creatorAddress = creatorAddress;
        this.forwarderAddress = forwarderAddress;
        this.generation = gossipDigest.getGeneration();
        this.version = gossipDigest.getVersion();
        this.creationTimestamp = gossipDigest.getCreationTimestamp();
        this.forwarderTimestamp = gossipDigest.getForwarderTimestamp();
        this.uid = gossipDigest.getUid();
        this.data = gossipDigest.getData();
        this.ttl = gossipDigest.getTtl();
        this.activeNeighbors = String.join(",", gossipDigest.getActiveNeighbors());
        this.inactiveNeighbors = String.join(",", gossipDigest.getInactiveNeighbors());
    }
}
