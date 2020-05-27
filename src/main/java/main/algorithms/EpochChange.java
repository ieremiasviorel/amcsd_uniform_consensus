package main.algorithms;

import main.Paxos;

public class EpochChange implements Algorithm {

    @Override
    public boolean canHandle(Paxos.Message message) {
        return false;
    }

    @Override
    public void doHandle(Paxos.Message message) {

    }
}
