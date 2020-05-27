package main.handlers;

import main.Paxos;
import main.algorithms.Algorithm;

import java.io.IOException;
import java.util.List;

public class EventLoop extends Thread {

    private final List<Paxos.Message> messageQueue;
    private final List<Algorithm> algorithms;

    public EventLoop(List<Paxos.Message> messageQueue, List<Algorithm> algorithms) {
        this.messageQueue = messageQueue;
        this.algorithms = algorithms;
    }

    @Override
    public void run() {
        while (true) {
            messageQueue.forEach(message -> {
                algorithms.forEach(algorithm -> {
                    try {
                        if (algorithm.handle(message)) {
                            logMessageInfo(message, algorithm);
                            messageQueue.remove(message);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            });
        }
    }

    private void logMessageInfo(Paxos.Message message, Algorithm algorithm) {
        if (getPayloadMessageType(message) != Paxos.Message.Type.EPFD_HEARTBEAT_REQUEST &&
                getPayloadMessageType(message) != Paxos.Message.Type.EPFD_HEARTBEAT_REPLY) {
            System.out.println("[" + messageQueue.size() + "] Message " +
                    message.getMessageUuid() + " [" + getPayloadMessageType(message) +
                    "] handled by " + algorithm.getClass().getSimpleName());
        }
    }

    private Paxos.Message.Type getPayloadMessageType(Paxos.Message message) {
        if (message.getType() == Paxos.Message.Type.PL_DELIVER) {
            return message.getPlDeliver().getMessage().getType();
        } else if (message.getType() == Paxos.Message.Type.PL_SEND) {
            return message.getPlSend().getMessage().getType();
        } else {
            return message.getType();
        }
    }
}
