package org.ncsu.entity;

import java.time.LocalDateTime;
import java.util.List;

public class GossipDigest {

    String uid;
    Long generation;
    Integer version;
    LocalDateTime creationTimestamp;
    LocalDateTime forwarderTimestamp;
    String data;
    Integer ttl;
    List<String> neighbors;;

    public GossipDigest()  {}

    public GossipDigest(String uid, Long generation, Integer version, LocalDateTime creationTimestamp,
                        LocalDateTime forwarderTimestamp, String data, Integer ttl, List<String> neighbors) {
        this.uid = uid;
        this.generation = generation;
        this.version = version;
        this.creationTimestamp = creationTimestamp;
        this.forwarderTimestamp = forwarderTimestamp;
        this.data = data;
        this.ttl = ttl;
        this.neighbors = neighbors;
    }

    public Long getGeneration() {
        return generation;
    }
    public void setGeneration(Long generation) {
        this.generation = generation;
    }

    public int getVersion() {
        return version;
    }
    public void setVersion(Integer version) {
        this.version = version;
    }

    public LocalDateTime getCreationTimestamp() {
        return creationTimestamp;
    }

    public void setCreationTimestamp(LocalDateTime creationTimestamp) {
        this.creationTimestamp = creationTimestamp;
    }

    public LocalDateTime getForwarderTimestamp() {
        return forwarderTimestamp;
    }

    public void setForwarderTimestamp(LocalDateTime forwarderTimestamp) {
        this.forwarderTimestamp = forwarderTimestamp;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public Integer getTtl() {
        return ttl;
    }

    public void setTtl(Integer ttl) {
        this.ttl = ttl;
    }

    public List<String> getNeighbors() {
        return neighbors;
    }

    public void setNeighbors(List<String> neighbors) {
        this.neighbors = neighbors;
    }
}
