package main.algorithms;

import main.Paxos;
import main.utils.EpochConsensusState;

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
        super("uc");
        val = Paxos.Value
                .newBuilder()
                .setDefined(false)
                .build();
        proposed = false;
        decided = false;

        l = system.getMaxRank(system.getProcesses());
        ets = 0;

        newl = null;
        newts = 0;

        Paxos.Value ep0StateValue = Paxos.Value
                .newBuilder()
                .setDefined(false)
                .build();
        EpochConsensusState ep0State = new EpochConsensusState(ep0StateValue, 0);
        EpochConsensus ep0 = new EpochConsensus(ep0State, l, ets);
        system.addAlgorithm(ep0);
    }

    @Override
    public boolean canHandle(Paxos.Message message) {
        /**
         * 1. UC_PROPOSE
         * 2. EC_START_EPOCH
         * 3. EPts_ABORTED
         * 4. EPts_DECIDE
         */
        return message.getType() == Paxos.Message.Type.UC_PROPOSE
                || message.getType() == Paxos.Message.Type.EC_START_EPOCH
                || (message.getType() == Paxos.Message.Type.EP_ABORTED && message.getEpAborted().getEts() == ets)
                || (message.getType() == Paxos.Message.Type.EP_DECIDE && message.getEpDecide().getEts() == ets);
    }

    @Override
    public void doHandle(Paxos.Message message) throws IOException {
        switch (message.getType()) {
            case UC_PROPOSE:
                handleUcPropose(message);
                break;
            case EC_START_EPOCH:
                handleEcStartEpoch(message);
                break;
            case EP_ABORTED:
                handleEpAborted(message);
                break;
            case EP_DECIDE:
                handleEpDecide(message);
                break;
        }
    }

    private void handleUcPropose(Paxos.Message message) {
        Paxos.UcPropose ucPropose = message.getUcPropose();
        val = ucPropose.getValue();
    }

    private void handleEcStartEpoch(Paxos.Message message) {
        Paxos.EcStartEpoch ecStartEpoch = message.getEcStartEpoch();
        newl = ecStartEpoch.getNewLeader();
        newts = ecStartEpoch.getNewTimestamp();
        sendEptsAbort();
    }

    private void handleEpAborted(Paxos.Message message) {
        Paxos.EpAborted epAborted = message.getEpAborted();

        l = newl;
        ets = newts;

        proposed = false;

        EpochConsensusState eptsState = new EpochConsensusState(epAborted.getValue(), epAborted.getValueTimestamp());
        EpochConsensus epts = new EpochConsensus(eptsState, l, ets);
        system.addAlgorithm(epts);
    }

    private void handleEpDecide(Paxos.Message message) {
        if (!decided) {
            decided = true;
            Paxos.EpDecide epDecide = message.getEpDecide();
            Paxos.Value value = epDecide.getValue();
            triggerUcDecideIndication(value);
        }
    }

    private void handleInternalEvent() {
        if (l.equals(system.getCurrentProcess()) && val.getDefined() && !proposed) {
            proposed = true;
        }
    }

    private void sendEptsAbort() {
        Paxos.EpAbort epAbort = Paxos.EpAbort
                .newBuilder()
                .build();

        Paxos.Message outerMessage = Paxos.Message
                .newBuilder()
                .setAbstractionId("ep" + ets)
                .setType(Paxos.Message.Type.EP_ABORT)
                .setEpAbort(epAbort)
                .build();

        system.addMessageToQueue(outerMessage);
    }

    private void triggerUcDecideIndication(Paxos.Value value) {
        Paxos.UcDecide ucDecide = Paxos.UcDecide
                .newBuilder()
                .setValue(value)
                .build();

        Paxos.Message outerMessage = builderWithIdentifierFields()
                .setType(Paxos.Message.Type.UC_DECIDE)
                .setUcDecide(ucDecide)
                .build();

        system.addMessageToQueue(outerMessage);
    }
}
