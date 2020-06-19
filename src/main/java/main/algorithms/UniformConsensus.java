package main.algorithms;

import main.Main;
import main.Paxos;

import java.io.IOException;

/**
 * Abstraction: UniformConsensus
 * Implementation: : LeaderDrivenConsensus
 */
public class UniformConsensus extends AbstractAlgorithm implements Algorithm {

    Paxos.Value val;
    boolean proposed;
    boolean decided;

    Paxos.ProcessId l;
    int ets;

    Paxos.ProcessId newl;
    int newts;

    public UniformConsensus() {
        val = Paxos.Value.newBuilder()
                .setDefined(false)
                .build();
        proposed = false;
        decided = false;

        l = system.getMaxRank(system.getProcesses());
        ets = 0;

        newl = null;
        newts = 0;
    }

    @Override
    String getAbstractionId() {
        return "uc";
    }

    @Override
    public boolean canHandle(Paxos.Message message) {
        return message.getType() == Paxos.Message.Type.EC_START_EPOCH;
    }

    @Override
    public void doHandle(Paxos.Message message) throws IOException {
        handleEcStartEpoch(message);
    }

    private void handleEcStartEpoch(Paxos.Message message) {
        Paxos.EcStartEpoch ecStartEpoch = message.getEcStartEpoch();

        newl = ecStartEpoch.getNewLeader();
        newts = ecStartEpoch.getNewTimestamp();
        System.out.println(l);
        System.out.println(ets);
        System.out.println(Main.ANSI_CYAN + "START EPOCH: leader " + ecStartEpoch.getNewLeader().getPort() + ", timeStamp:" + ecStartEpoch.getNewTimestamp() + Main.ANSI_RESET);
    }
}
