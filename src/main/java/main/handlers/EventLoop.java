package main.handlers;

import main.Paxos;
import main.algorithms.Algorithm;

import java.io.IOException;
import java.util.List;

public class EventLoop extends Thread {
    public final List<Paxos.Message> messageQueue;
    public final List<Algorithm> algorithms;

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
                            System.out.println("[" + messageQueue.size() + "] Message " + message.getMessageUuid() + " [" + getMeaningfulMessageType(message) + "] handled by " + algorithm.getClass().getSimpleName());
                            messageQueue.remove(message);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            });
        }
    }

    private Paxos.Message.Type getMeaningfulMessageType(Paxos.Message message) {
        if (message.getType() == Paxos.Message.Type.PL_DELIVER) {
            return message.getPlDeliver().getMessage().getType();
        } else if (message.getType() == Paxos.Message.Type.PL_SEND) {
            return message.getPlSend().getMessage().getType();
        } else {
            return message.getType();
        }
    }
}
