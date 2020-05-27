package main.algorithms;

import main.Paxos;

import java.io.IOException;

public interface Algorithm {

    default boolean handle(Paxos.Message message) throws IOException {
        if (canHandle(message)) {
            doHandle(message);
            return true;
        }
        return false;
    }

    boolean canHandle(Paxos.Message message);

    void doHandle(Paxos.Message message) throws IOException;
}
