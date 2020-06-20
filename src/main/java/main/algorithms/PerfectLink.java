package main.algorithms;

import main.Paxos;

import java.io.IOException;

public class PerfectLink extends AbstractAlgorithm implements Algorithm {

    public PerfectLink() {
        super("pl");
    }

    @Override
    public boolean canHandle(Paxos.Message message) {
        /**
         * 1. PL_SEND
         */
        return message.getType() == Paxos.Message.Type.PL_SEND;
    }

    @Override
    public void doHandle(Paxos.Message message) throws IOException {
        Paxos.ProcessId messageDestination = message.getPlSend().getDestination();
        system.sendMessageOverTheNetwork(
                message.getPlSend().getMessage(),
                messageDestination.getHost(),
                messageDestination.getPort()
        );
    }
}
