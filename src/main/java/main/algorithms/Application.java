package main.algorithms;

import main.ConsensusSystem;
import main.Paxos;

public class Application extends AbstractAlgorithm implements Algorithm {

    @Override
    public boolean canHandle(Paxos.Message message) {
        return message.getType() == Paxos.Message.Type.PL_DELIVER &&
                message.getPlDeliver().getMessage().getType() == Paxos.Message.Type.APP_PROPOSE;
    }

    @Override
    public void doHandle(Paxos.Message message) {
        Paxos.AppPropose appPropose = message.getPlDeliver().getMessage().getAppPropose();

        ConsensusSystem consensusSystemInstance = ConsensusSystem.getInstance();

        consensusSystemInstance.setProcessList(appPropose.getProcessesList());
        consensusSystemInstance.setSystemId(message.getSystemId());
        consensusSystemInstance.initializeDefaultAlgorithms();
    }
}
