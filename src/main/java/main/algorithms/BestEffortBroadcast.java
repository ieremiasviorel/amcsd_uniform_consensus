package main.algorithms;

import main.Paxos;

import java.io.IOException;

public class BestEffortBroadcast extends AbstractAlgorithm implements Algorithm {
    @Override
    String getAbstractionId() {
        return "beb";
    }

    @Override
    public boolean canHandle(Paxos.Message message) {
        return message.getType() == Paxos.Message.Type.BEB_BROADCAST;
    }

    @Override
    public void doHandle(Paxos.Message message) throws IOException {
        system.getProcesses().forEach(processId -> {
            Paxos.PlSend plSend = Paxos.PlSend
                    .newBuilder()
                    .setDestination(processId)
                    .setMessage(message)
                    .build();

            Paxos.Message outerMessage = builderWithIdentifierFields()
                    .setType(Paxos.Message.Type.PL_SEND)
                    .setPlSend(plSend)
                    .build();

            system.addMessageToQueue(outerMessage);
        });
    }
}
