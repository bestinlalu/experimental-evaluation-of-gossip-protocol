package org.ncsu.entity;

public class GossipEvent {

    String creatorAddress;
    String forwarderAddress;
    String strategy;
    GossipDigest gossipDigest;

    public GossipEvent() {
    }

    public GossipEvent(String creatorAddress, String forwarderAddress, GossipDigest gossipDigest) {
        this.creatorAddress = creatorAddress;
        this.forwarderAddress = forwarderAddress;
        this.gossipDigest = gossipDigest;
    }

    public String getCreatorAddress() {
        return creatorAddress;
    }

    public void setCreatorAddress(String creatorAddress) {
        this.creatorAddress = creatorAddress;
    }

    public String getForwarderAddress() {
        return forwarderAddress;
    }

    public void setForwarderAddress(String forwarderAddress) {
        this.forwarderAddress = forwarderAddress;
    }

    public String getStrategy() {
        return strategy;
    }

    public void setStrategy(String strategy) {
        this.strategy = strategy;
    }

    public GossipDigest getGossipDigest() {
        return gossipDigest;
    }

    public void setGossipDigest(GossipDigest gossipDigest) {
        this.gossipDigest = gossipDigest;
    }
}
