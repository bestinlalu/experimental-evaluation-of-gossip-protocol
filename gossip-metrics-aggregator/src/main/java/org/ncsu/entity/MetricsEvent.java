package org.ncsu.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.persistence.Entity;

import java.time.LocalDateTime;

@Entity
public class MetricsEvent extends PanacheEntity {
    String nodeAddress;
    Float cpuPercentage;
    Float memoryPercentage;
    Float io_read_mbytes;
    Float io_write_mbytes;
    LocalDateTime timestamp;

    public MetricsEvent() {}

    public MetricsEvent(String nodeAddress, Float cpuPercentage, Float memoryPercentage, Float io_read_mbytes, Float io_write_mbytes, LocalDateTime timestamp) {
        this.nodeAddress = nodeAddress;
        this.cpuPercentage = cpuPercentage;
        this.memoryPercentage = memoryPercentage;
        this.io_read_mbytes = io_read_mbytes;
        this.io_write_mbytes = io_write_mbytes;
        this.timestamp = timestamp;
    }

    public String getNodeAddress() {
        return nodeAddress;
    }

    public void setNodeAddress(String nodeAddress) {
        this.nodeAddress = nodeAddress;
    }

    public Float getCpuPercentage() {
        return cpuPercentage;
    }

    public void setCpuPercentage(Float cpuPercentage) {
        this.cpuPercentage = cpuPercentage;
    }

    public Float getMemoryPercentage() {
        return memoryPercentage;
    }

    public void setMemoryPercentage(Float memoryPercentage) {
        this.memoryPercentage = memoryPercentage;
    }

    public Float getIo_read_mbytes() {
        return io_read_mbytes;
    }

    public void setIo_read_mbytes(Float io_read_mbytes) {
        this.io_read_mbytes = io_read_mbytes;
    }

    public Float getIo_write_mbytes() {
        return io_write_mbytes;
    }

    public void setIo_write_mbytes(Float io_write_mbytes) {
        this.io_write_mbytes = io_write_mbytes;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }
}
