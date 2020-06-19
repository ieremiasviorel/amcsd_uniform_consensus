package main.algorithms;

import main.ConsensusSystem;
import main.Paxos;
import main.utils.SetOperations;

import java.util.HashSet;
import java.util.Set;

/**
 * Abstraction: EventuallyPerfectFailureDetector
 * Implementation: IncreasingTimeout
 */
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
        /**
         * 1. EPFD_HEARTBEAT_REQUEST wrapped in a PL_DELIVER
         * 2. EPFD_HEARBEAT_REPLY wrapped in a PL_DELIVER
         * 3. EPFD_TIMEOUT
         */
        return message.getType() == Paxos.Message.Type.PL_DELIVER && (
                message.getPlDeliver().getMessage().getType() == Paxos.Message.Type.EPFD_HEARTBEAT_REQUEST ||
                        message.getPlDeliver().getMessage().getType() == Paxos.Message.Type.EPFD_HEARTBEAT_REPLY) ||
                message.getType() == Paxos.Message.Type.EPFD_TIMEOUT;
    }

    @Override
    public void doHandle(Paxos.Message message) {
        Paxos.Message.Type messageType;

        if (message.getType() == Paxos.Message.Type.PL_DELIVER) {
            messageType = message.getPlDeliver().getMessage().getType();
        } else {
            messageType = message.getType();
        }

        switch (messageType) {
            case EPFD_HEARTBEAT_REQUEST:
                handleHeartbeatRequest(message);
                break;
            case EPFD_HEARTBEAT_REPLY:
                handleHeartbeatReply(message);
                break;
            case EPFD_TIMEOUT:
                handleTimeout(message);
                break;
        }
    }

    private void handleHeartbeatRequest(Paxos.Message message) {
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

    private void handleHeartbeatReply(Paxos.Message message) {
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
                triggerSuspectIndication(processId);
            } else if (SetOperations.belongs(processId, alive) && SetOperations.belongs(processId, suspected)) {
                suspected.remove(processId);
                triggerRestoreIndication(processId);
            }
            sendHeartbeatRequest(processId);
        });

        alive.clear();
        system.setTimer(delay, Paxos.Message.Type.EPFD_TIMEOUT);
    }

    private void triggerSuspectIndication(Paxos.ProcessId processId) {
        Paxos.EpfdSuspect epfdSuspect = Paxos.EpfdSuspect
                .newBuilder()
                .setProcess(processId)
                .build();

        Paxos.Message outerMessage = builderWithIdentifierFields()
                .setType(Paxos.Message.Type.EPFD_SUSPECT)
                .setEpfdSuspect(epfdSuspect)
                .build();

        system.addMessageToQueue(outerMessage);
    }

    private void triggerRestoreIndication(Paxos.ProcessId processId) {
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

    private void sendHeartbeatRequest(Paxos.ProcessId processId) {
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
    }
}
