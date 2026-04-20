package org.ncsu.entity;

import java.time.LocalDateTime;

public class GossipDigest {

    String uid;
    Long generation;
    Integer version;
    LocalDateTime creationTimestamp;
    LocalDateTime forwarderTimestamp;
    String data;
    Integer ttl;

    public GossipDigest()  {}

    public GossipDigest(String uid, Long generation, Integer version, LocalDateTime creationTimestamp,
                        LocalDateTime forwarderTimestamp, String data, Integer ttl) {
        this.uid = uid;
        this.generation = generation;
        this.version = version;
        this.creationTimestamp = creationTimestamp;
        this.forwarderTimestamp = forwarderTimestamp;
        this.data = data;
        this.ttl = ttl;
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
}
