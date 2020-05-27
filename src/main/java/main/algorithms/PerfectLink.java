package main.algorithms;

import main.Paxos;

import java.io.IOException;

public class PerfectLink extends AbstractAlgorithm implements Algorithm {

    @Override
    public boolean canHandle(Paxos.Message message) {
        return message.getType() == Paxos.Message.Type.PL_SEND;
    }

    @Override
    public void doHandle(Paxos.Message message) throws IOException {
        Paxos.ProcessId messageDestination = message.getPlSend().getDestination();
        sendNetworkMessage(message, messageDestination);
    }
}
