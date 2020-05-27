package main.algorithms;

import main.ConsensusSystem;
import main.Paxos;

import java.util.HashSet;
import java.util.Set;

public class EventuallyPerfectFailureDetector extends AbstractAlgorithm implements Algorithm {

    private final Set<Paxos.ProcessId> alive;
    private final Set<Paxos.ProcessId> suspected;

    public EventuallyPerfectFailureDetector() {
        alive = new HashSet<>(ConsensusSystem.getInstance().getProcessList());
        suspected = new HashSet<>();
    }


    @Override
    public boolean canHandle(Paxos.Message message) {
        return
                message.getType() == Paxos.Message.Type.PL_DELIVER &&
                        (message.getPlDeliver().getMessage().getType() == Paxos.Message.Type.EPFD_HEARTBEAT_REQUEST ||
                                message.getPlDeliver().getMessage().getType() == Paxos.Message.Type.EPFD_HEARTBEAT_REPLY
                        );
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
        }
    }

    private void handleRequest(Paxos.Message message) {
        Paxos.ProcessId sender = message.getPlDeliver().getSender();

        Paxos.EpfdHeartbeatReply_ epfdHeartbeatReply = Paxos.EpfdHeartbeatReply_
                .newBuilder()
                .build();

        Paxos.Message innerMessage = Paxos.Message
                .newBuilder()
                .setType(Paxos.Message.Type.EPFD_HEARTBEAT_REPLY)
                .setEpfdHeartbeatReply(epfdHeartbeatReply)
                .setAbstractionId("app")
                .setMessageUuid("appRegistration")
                .build();

        Paxos.PlSend plSend = Paxos.PlSend
                .newBuilder()
                .setDestination(sender)
                .setMessage(innerMessage)
                .build();

        Paxos.Message outerMessage = Paxos.Message
                .newBuilder()
                .setType(Paxos.Message.Type.PL_SEND)
                .setPlSend(plSend)
                .build();

        addQueueMessage(outerMessage);
    }

    private void handleReply(Paxos.Message message) {
        Paxos.ProcessId sender = message.getPlDeliver().getSender();
        alive.add(sender);
    }
}
