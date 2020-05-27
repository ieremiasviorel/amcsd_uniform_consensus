package main.algorithms;

import main.Paxos;
import main.utils.SetOperations;

import java.util.HashSet;
import java.util.Set;

public class EventualLeaderDetector extends AbstractAlgorithm implements Algorithm {

    private final Set<Paxos.ProcessId> suspected;
    private Paxos.ProcessId leader;

    public EventualLeaderDetector() {
        suspected = new HashSet<>();
        leader = null;
    }

    @Override
    String getAbstractionId() {
        return "eld";
    }

    @Override
    public boolean canHandle(Paxos.Message message) {
        return message.getType() == Paxos.Message.Type.EPFD_SUSPECT ||
                message.getType() == Paxos.Message.Type.EPFD_RESTORE;
    }

    @Override
    public void doHandle(Paxos.Message message) {
        switch (message.getType()) {
            case EPFD_SUSPECT:
                handleSuspect(message);
                break;
            case EPFD_RESTORE:
                handleRestore(message);
                break;
        }
    }

    private void handleRestore(Paxos.Message message) {
        handleInternalEvent();
        suspected.add(message.getEpfdRestore().getProcess());
    }

    private void handleSuspect(Paxos.Message message) {
        handleInternalEvent();
        suspected.remove(message.getEpfdRestore().getProcess());
    }

    private void handleInternalEvent() {
        Set<Paxos.ProcessId> notSuspectedProcesses =
                SetOperations.difference(system.getProcesses(), suspected);

        Paxos.ProcessId maxRankProcess = system.getMaxRank(notSuspectedProcesses);

        if (!maxRankProcess.equals(leader)) {
            leader = maxRankProcess;

            System.out.println("TRUST " + leader.getPort());

            Paxos.EldTrust eldTrust = Paxos.EldTrust
                    .newBuilder()
                    .setProcess(leader)
                    .build();

            Paxos.Message outerMessage = builderWithIdentifierFields()
                    .setType(Paxos.Message.Type.ELD_TRUST)
                    .setEldTrust(eldTrust)
                    .build();

            system.addMessageToQueue(outerMessage);
        }
    }
}