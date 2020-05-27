package main.algorithms;

import main.ConsensusSystem;
import main.Paxos;
import main.utils.SetOperations;

import java.util.HashSet;
import java.util.Set;

public class EventuallyPerfectFailureDetector extends AbstractAlgorithm implements Algorithm {

    private final Set<Paxos.ProcessId> alive;
    private final Set<Paxos.ProcessId> suspected;
    private final int delta = 200;
    private int delay;

    public EventuallyPerfectFailureDetector() {
        alive = new HashSet<>(ConsensusSystem.getInstance().getProcesses());
        suspected = new HashSet<>();
        delay = delta;
        system.setTimer(delay, Paxos.Message.Type.EPFD_TIMEOUT);
    }

    @Override
    String getAbstractionId() {
        return "epfd";
    }

    @Override
    public boolean canHandle(Paxos.Message message) {
        return message.getType() == Paxos.Message.Type.PL_DELIVER && (
                message.getPlDeliver().getMessage().getType() == Paxos.Message.Type.EPFD_HEARTBEAT_REQUEST ||
                        message.getPlDeliver().getMessage().getType() == Paxos.Message.Type.EPFD_HEARTBEAT_REPLY) ||
                message.getType() == Paxos.Message.Type.EPFD_TIMEOUT;
    }

    @Override
    public void doHandle(Paxos.Message message) {
        switch (message.getPlDeliver().getMessage().getType()) {
            case EPFD_HEARTBEAT_REQUEST:
                handleRequest(message);
                break;
            case EPFD_HEARTBEAT_REPLY:
                handleReply(message);
                break;
            case EPFD_TIMEOUT:
                handleTimeout(message);
                break;
        }
    }

    private void handleRequest(Paxos.Message message) {
        Paxos.ProcessId sender = message.getPlDeliver().getSender();

        Paxos.EpfdHeartbeatReply_ epfdHeartbeatReply = Paxos.EpfdHeartbeatReply_
                .newBuilder()
                .build();

        Paxos.Message innerMessage = builderWithIdentifierFields()
                .setType(Paxos.Message.Type.EPFD_HEARTBEAT_REPLY)
                .setEpfdHeartbeatReply(epfdHeartbeatReply)
                .build();

        Paxos.PlSend plSend = Paxos.PlSend
                .newBuilder()
                .setDestination(sender)
                .setMessage(innerMessage)
                .build();

        Paxos.Message outerMessage = builderWithIdentifierFields()
                .setType(Paxos.Message.Type.PL_SEND)
                .setPlSend(plSend)
                .build();

        system.addMessageToQueue(outerMessage);
    }

    private void handleReply(Paxos.Message message) {
        Paxos.ProcessId sender = message.getPlDeliver().getSender();
        alive.add(sender);
    }

    private void handleTimeout(Paxos.Message message) {
        if (SetOperations.intersection(alive, suspected).size() != 0) {
            delay += delta;
        }

        system.getProcesses().forEach(processId -> {
            if (!SetOperations.belongs(processId, alive) && !SetOperations.belongs(processId, suspected)) {
                suspected.add(processId);

                Paxos.EpfdSuspect epfdSuspect = Paxos.EpfdSuspect
                        .newBuilder()
                        .setProcess(processId)
                        .build();

                Paxos.Message outerMessage = builderWithIdentifierFields()
                        .setType(Paxos.Message.Type.EPFD_SUSPECT)
                        .setEpfdSuspect(epfdSuspect)
                        .build();

                system.addMessageToQueue(outerMessage);

            } else if (SetOperations.belongs(processId, alive) && SetOperations.belongs(processId, suspected)) {
                Paxos.EpfdRestore epfdRestore = Paxos.EpfdRestore
                        .newBuilder()
                        .setProcess(processId)
                        .build();

                Paxos.Message outerMessage = builderWithIdentifierFields()
                        .setType(Paxos.Message.Type.EPFD_RESTORE)
                        .setEpfdRestore(epfdRestore)
                        .build();

                system.addMessageToQueue(outerMessage);

            }

            Paxos.EpfdHeartbeatRequest_ epfdHeartbeatRequest = Paxos.EpfdHeartbeatRequest_
                    .newBuilder()
                    .build();

            Paxos.Message innerMessage = builderWithIdentifierFields()
                    .setType(Paxos.Message.Type.EPFD_HEARTBEAT_REQUEST)
                    .setEpfdHeartbeatRequest(epfdHeartbeatRequest)
                    .build();

            Paxos.PlSend plSend = Paxos.PlSend
                    .newBuilder()
                    .setDestination(processId)
                    .setMessage(innerMessage)
                    .build();

            Paxos.Message outerMessage = builderWithIdentifierFields()
                    .setType(Paxos.Message.Type.PL_SEND)
                    .setPlSend(plSend)
                    .build();

            system.addMessageToQueue(outerMessage);
        });

        alive.clear();
        system.setTimer(delay, Paxos.Message.Type.EPFD_TIMEOUT);
    }
}
