package main.algorithms;

import main.ConsensusSystem;
import main.Paxos;

public class Application implements Algorithm {

    @Override
    public boolean handle(Paxos.Message message) {
        if (message.getType() == Paxos.Message.Type.APP_PROPOSE) {
            Paxos.AppPropose appPropose = message.getAppPropose();
            ConsensusSystem.setProcessList(appPropose.getProcessesList());

            return true;
        }
        return false;
    }
}
