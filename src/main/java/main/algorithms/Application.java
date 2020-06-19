package main.algorithms;

import main.Paxos;

import java.util.HashSet;

public class Application extends AbstractAlgorithm implements Algorithm {

    @Override
    String getAbstractionId() {
        return "app";
    }

    @Override
    public boolean canHandle(Paxos.Message message) {
        return message.getType() == Paxos.Message.Type.PL_DELIVER &&
                message.getPlDeliver().getMessage().getType() == Paxos.Message.Type.APP_PROPOSE;
    }

    @Override
    public void doHandle(Paxos.Message message) {
        Paxos.AppPropose appPropose = message.getPlDeliver().getMessage().getAppPropose();

        system.setProcesses(new HashSet<>(appPropose.getProcessesList()));
        system.initializeDefaultAlgorithms();
    }
}
