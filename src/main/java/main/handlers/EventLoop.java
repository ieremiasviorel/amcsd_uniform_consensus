package main.handlers;

import main.Paxos;
import main.algorithms.Algorithm;

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
                    if (algorithm.handle(message)) {
                        System.out.println("Message " + message.getMessageUuid() + " [" + message.getType() + "] handled by " + algorithm.getClass().getName());
                        messageQueue.remove(message);
                    }
                });
            });
        }
    }
}
