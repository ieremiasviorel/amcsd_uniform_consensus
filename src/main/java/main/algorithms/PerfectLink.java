package main.algorithms;

import main.Paxos;

import java.io.IOException;

public class PerfectLink extends AbstractAlgorithm implements Algorithm {

    @Override
    String getAbstractionId() {
        return "pl";
    }

    @Override
    public boolean canHandle(Paxos.Message message) {
        return message.getType() == Paxos.Message.Type.PL_SEND;
    }

    @Override
    public void doHandle(Paxos.Message message) throws IOException {
        Paxos.ProcessId messageDestination = message.getPlSend().getDestination();
        system.sendMessageOverTheNetwork(message, messageDestination.getHost(), messageDestination.getPort());
    }
}
