package main.algorithms;

import main.Paxos;

public interface Algorithm {

    boolean handle(Paxos.Message message);
}
