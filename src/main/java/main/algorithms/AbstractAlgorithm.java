package main.algorithms;

import main.ConsensusSystem;
import main.Paxos;
import main.handlers.NetworkHandler;

import java.io.IOException;
import java.util.List;

public class AbstractAlgorithm {

    private final NetworkHandler networkHandler;
    private final List<Paxos.Message> messageQueue;

    protected AbstractAlgorithm() {
        ConsensusSystem system = ConsensusSystem.getInstance();
        networkHandler = system.getNetworkHandler();
        messageQueue = system.getMessageQueue();
    }

    protected void addQueueMessage(Paxos.Message message) {
        messageQueue.add(message);
    }

    protected void sendNetworkMessage(Paxos.Message message, Paxos.ProcessId destination) throws IOException {
        networkHandler.sendMessage(message, destination.getHost(), destination.getPort());
    }
}
