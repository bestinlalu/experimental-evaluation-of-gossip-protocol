package org.ncsu.service;

import io.quarkus.runtime.ShutdownEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped // CRITICAL: Makes this a singleton service
public class GossipProcessManager {

    private final Map<Integer, Process> activeNodes = new ConcurrentHashMap<>();
    private final Map<Integer, Integer> nodeRestarts = new HashMap<>();
    private final Map<Integer, Process> activeTelemetrys = new ConcurrentHashMap<>();

    public void startNode(String id, String address, Integer port, List<String> peers, String kafkaTopic, String kafkaBroker, String strategy) throws Exception {
        if (activeNodes.containsKey(port) && activeNodes.get(port).isAlive()) {
            throw new IllegalStateException("Node already running on port " + port);
        }

        String peerAddress = "";
        if (peers != null && !peers.isEmpty()) {
            peerAddress = String.join(",", peers);;
        }

        List<String> command = new ArrayList<>();
        command.add("go");
        command.add("run");
        if(strategy.equalsIgnoreCase("push")) {
            command.add("../node-scripts/push/main.go");
        } else if(strategy.equalsIgnoreCase("pull")) {
            command.add("../node-scripts/pull/main.go");
        } else if(strategy.equalsIgnoreCase("pushpull")) {
            command.add("../node-scripts/push-pull/main.go");
        } else {
            throw new Exception("Invalid strategy: " + strategy);
        }
//        command.add("main.go");
        command.add("-id");
        command.add(id);
        command.add("-addr");
        command.add(address + ":" + port);

        if (!peerAddress.isEmpty()) {
            command.add("-peers");
            command.add(peerAddress);
        }
        if (kafkaBroker != null && !kafkaBroker.isBlank()) {
            command.add("-kafka-broker");
            command.add(kafkaBroker);
            if (kafkaTopic != null && !kafkaTopic.isBlank()) {
                command.add("-kafka-topic");
                command.add(kafkaTopic);
            }
        }
        if (nodeRestarts.containsKey(port)) {
            nodeRestarts.put(port, nodeRestarts.get(port) + 1);
            command.add("-gen");
            command.add(Integer.toString(nodeRestarts.get(port)));
        } else {
            command.add("-gen");
            command.add(String.valueOf(0));
            nodeRestarts.put(port, 0);
        }

        System.out.println("DEBUG: Raw peer string before ProcessBuilder: " + address);

        List<String> telemetryCommand = new ArrayList<>();
        telemetryCommand.add("python3");
        telemetryCommand.add("../node-scripts/node_telemetry/start_node_telemetry.py");
        telemetryCommand.add("-addr");
        telemetryCommand.add(address + ":" + port);
        telemetryCommand.add("-kafka-broker");
        telemetryCommand.add(kafkaBroker);

        ProcessBuilder pb = new ProcessBuilder(command);
        ProcessBuilder pbtl = new ProcessBuilder(telemetryCommand);
        // pb.directory(new File("/path/to/your/go/project"));
        pb.inheritIO();
        pbtl.inheritIO();

        Process process = pb.start();
        Process telemetry = pbtl.start();

        if (!process.isAlive() && process.exitValue() != 0) {
            telemetry.destroyForcibly();
            throw new RuntimeException("Go process terminated immediately upon starting.");
        }

        activeNodes.put(port, process);
        activeTelemetrys.put(port, telemetry);
    }

    public boolean killNode(Integer port) {
        Process process = activeNodes.remove(port);
        Process telemetry = activeTelemetrys.remove(port);
        if (process != null && process.isAlive()) {
            System.out.println("Killing Node on port: " + port);
            // 1. Kill all child processes (e.g., the actual compiled Go binary)
            process.descendants().forEach(ProcessHandle::destroyForcibly);
            process.destroyForcibly();

            if (telemetry != null && telemetry.isAlive()) {
                telemetry.descendants().forEach(ProcessHandle::destroyForcibly);
                telemetry.destroyForcibly();
            }
            return true;
        }
        return false;
    }

    public boolean killAll() {
        if (activeNodes.isEmpty()) {
            System.out.println("No active nodes");
            return false;
        }

        for (Integer port : activeNodes.keySet()) {
            killNode(port);
        }
        return true;
    }

    /**
     * Automatically cleans up orphaned processes when Quarkus shuts down.
     */
    void onStop(@Observes ShutdownEvent ev) {
        if (activeNodes.isEmpty()) return;

        System.out.println("Quarkus is shutting down. Terminating " + activeNodes.size() + " active gossip nodes...");

        for (Map.Entry<Integer, Process> entry : activeNodes.entrySet()) {
            Process process = entry.getValue();
            if (process != null && process.isAlive()) {
                System.out.println("Killing Node on port: " + entry.getKey());
                // 1. Kill all child processes (e.g., the actual compiled Go binary)
                process.descendants().forEach(ProcessHandle::destroyForcibly);
                process.destroyForcibly();
            }
        }
        activeNodes.clear();
        System.out.println("All gossip nodes terminated.");
    }
}
