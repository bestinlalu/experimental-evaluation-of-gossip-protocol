package org.ncsu.entity;

import java.time.LocalDateTime;

public class GossipDigest {

    String uid;
    Long generation;
    Integer version;
    LocalDateTime timestamp;
    String data;
    Integer ttl;

    public GossipDigest()  {}

    public GossipDigest(String uid, Long generation, Integer version, LocalDateTime timestamp,
                        String data, Integer ttl) {
        this.uid = uid;
        this.generation = generation;
        this.version = version;
        this.timestamp = timestamp;
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

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
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
