package main.algorithms;

import main.Paxos;

import java.io.IOException;

public class UniformConsensus implements Algorithm {

    @Override
    public boolean canHandle(Paxos.Message message) {
        return false;
    }

    @Override
    public void doHandle(Paxos.Message message) throws IOException {

    }
}
