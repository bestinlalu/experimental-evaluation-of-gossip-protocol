package org.ncsu.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;

import java.time.LocalDateTime;
import java.util.List;

@Entity
public class ActionRecord extends PanacheEntity {

    String host;
    Integer managerPort;
    Integer actionPort;
    List<String> peers;
    String kafkaBroker;
    String topic;
    Integer status;
    LocalDateTime timestamp;
    @Enumerated(EnumType.STRING)
    Strategy strategy;
    @Enumerated(EnumType.STRING)
    Action action;

    public ActionRecord() {}

    public ActionRecord(String nodeAddress, Integer managerPort) {
        this.host = nodeAddress;
        this.managerPort = managerPort;
    }

    public ActionRecord(String nodeAddress, Integer managerPort, Integer actionPort, List<String> peers,
                        Strategy strategy, String kafkaBroker, String topic) {
        this.host = nodeAddress;
        this.managerPort = managerPort;
        this.actionPort = actionPort;
        this.peers = peers;
        this.strategy = strategy;
        this.kafkaBroker = kafkaBroker;
        this.topic = topic;
    }

    public ActionRecord(String nodeAddress, Integer managerPort, Integer actionPort, List<String> peers, String kafkaBroker,
                        String topic, Integer status, LocalDateTime timestamp, Strategy strategy, Action action) {
        this.host = nodeAddress;
        this.managerPort = managerPort;
        this.actionPort = actionPort;
        this.peers = peers;
        this.kafkaBroker = kafkaBroker;
        this.topic = topic;
        this.status = status;
        this.timestamp = timestamp;
        this.strategy = strategy;
        this.action = action;
    }

    public String getHost() {
        return host;
    }
    public void setHost(String nodeAddress) {
        this.host = nodeAddress;
    }

    public Integer getManagerPort() {
        return managerPort;
    }

    public void setManagerPort(Integer managerPort) {
        this.managerPort = managerPort;
    }

    public Integer getActionPort() {
        return actionPort;
    }

    public void setActionPort(Integer actionPort) {
        this.actionPort = actionPort;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    public Action getAction() {
        return action;
    }

    public void setAction(Action action) {
        this.action = action;
    }

    public List<String> getPeers() {
        return peers;
    }

    public void setPeers(List<String> peers) {
        this.peers = peers;
    }

    public String getKafkaBroker() {
        return kafkaBroker;
    }

    public void setKafkaBroker(String kafkaBroker) {
        this.kafkaBroker = kafkaBroker;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Strategy getStrategy() {
        return strategy;
    }
    public void setStrategy(Strategy strategy) {
        this.strategy = strategy;
    }
}
