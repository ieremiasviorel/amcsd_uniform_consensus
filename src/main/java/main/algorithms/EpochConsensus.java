package main.algorithms;

import main.Paxos;
import main.utils.EpochConsensusState;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Abstraction: EpochConsensus
 * Implementation: ReadWriteEpochConsensus
 */
public class EpochConsensus extends AbstractAlgorithm implements Algorithm {

    EpochConsensusState state;
    int ets;
    Paxos.Value tmpval;
    Map<Paxos.ProcessId, EpochConsensusState> states;
    int accepted;

    boolean active;

    public EpochConsensus(EpochConsensusState state, Paxos.ProcessId leader, int ets) {
        super("ep" + ets);
        this.state = state;
        this.ets = ets;
        this.tmpval = Paxos.Value
                .newBuilder()
                .setDefined(false)
                .build();
        this.states = new HashMap<>();
        this.accepted = 0;

        active = true;
    }

    @Override
    public boolean canHandle(Paxos.Message message) {
        /**
         * 1. EP_PROPOSE
         * 2. EP_ABORT
         * 3. EP_READ wrapped in a PL_DELIVER
         * 4. EP_STATE wrapped in a PL_DELIVER
         * 5. EP_WRITE wrapped in a PL_DELIVER
         * 6. EP_ACCEPT wrapped in a PL_DELIVER
         * 7. EP_DECIDED wrapped in a PL_DELIVER
         */
        return active && (message.getType() == Paxos.Message.Type.EP_PROPOSE
                || message.getType() == Paxos.Message.Type.EP_ABORT
                || (message.getType() == Paxos.Message.Type.PL_DELIVER && (
                message.getPlDeliver().getMessage().getType() == Paxos.Message.Type.EP_READ_
                        || message.getPlDeliver().getMessage().getType() == Paxos.Message.Type.EP_STATE_
                        || message.getPlDeliver().getMessage().getType() == Paxos.Message.Type.EP_WRITE_
                        || message.getPlDeliver().getMessage().getType() == Paxos.Message.Type.EP_ACCEPT_
                        || message.getPlDeliver().getMessage().getType() == Paxos.Message.Type.EP_DECIDED_)));
    }

    @Override
    public void doHandle(Paxos.Message message) {
        Paxos.Message.Type messageType;

        if (message.getType() == Paxos.Message.Type.PL_DELIVER) {
            messageType = message.getPlDeliver().getMessage().getType();
        } else {
            messageType = message.getType();
        }

        switch (messageType) {
            /**
             * ONLY RECEIVED BY CURRENT LEADER
             */
            case EP_PROPOSE:
                handleEpPropose(message);
                break;
            case EP_STATE_:
                handleEpState(message);
                break;
            case EP_ACCEPT_:
                handleEpAccept(message);
                break;
            /**
             * RECEIVED BY ALL NODES
             */
            case EP_READ_:
                handleEpRead(message);
                break;
            case EP_WRITE_:
                handleEpWrite(message);
                break;
            case EP_DECIDED_:
                handleEpDecided(message);
                break;
            case EP_ABORT:
                handleEpAbort(message);
        }
    }

    /**
     * ONLY RECEIVED BY CURRENT LEADER
     */
    private void handleEpPropose(Paxos.Message message) {
        Paxos.EpPropose epPropose = message.getEpPropose();
        tmpval = epPropose.getValue();
        broadcastEpRead();
    }

    private void handleEpState(Paxos.Message message) {
        Paxos.PlDeliver plDeliver = message.getPlDeliver();
        Paxos.ProcessId sender = plDeliver.getSender();
        Paxos.EpState_ epState_ = plDeliver.getMessage().getEpState();
        EpochConsensusState state = new EpochConsensusState(epState_.getValue(), epState_.getValueTimestamp());
        states.put(sender, state);

        handleStatesThreshold();
    }

    private void handleEpAccept(Paxos.Message message) {
        accepted += 1;

        handleAcceptedThreshold();
    }

    /**
     * RECEIVED BY ALL NODES
     */
    private void handleEpRead(Paxos.Message message) {
        Paxos.ProcessId sender = message.getPlDeliver().getSender();
        sendEpState(sender);
    }

    private void handleEpWrite(Paxos.Message message) {
        Paxos.PlDeliver plDeliver = message.getPlDeliver();
        Paxos.ProcessId sender = plDeliver.getSender();
        Paxos.EpWrite_ epWrite_ = plDeliver.getMessage().getEpWrite();
        state = new EpochConsensusState(epWrite_.getValue(), ets);
        sendEpAccept(sender);
    }

    private void handleEpDecided(Paxos.Message message) {
        Paxos.PlDeliver plDeliver = message.getPlDeliver();
        Paxos.EpDecided_ epDecided_ = plDeliver.getMessage().getEpDecided();
        triggerEpDecideIndication(epDecided_.getValue());
    }

    private void handleEpAbort(Paxos.Message message) {
        triggerEpAbortedIndication();
        active = false;
    }

    /**
     * INTERNAL EVENTS
     */
    private void handleStatesThreshold() {
        if (states.size() > system.getProcesses().size() / 2) {
            EpochConsensusState stateToWrite = highestEpochState();
            if (stateToWrite.val.getDefined()) {
                tmpval = stateToWrite.val;
            }
            states.clear();
            this.broadcastEpWrite();
        }
    }

    private void handleAcceptedThreshold() {
        if (accepted > system.getProcesses().size() / 2) {
            accepted = 0;
            broadcastEpDecided();
        }
    }

    /**
     * SENDS AND BROADCASTS
     */
    private void broadcastEpRead() {
        Paxos.EpRead_ epRead_ = Paxos.EpRead_
                .newBuilder()
                .build();

        Paxos.Message innerMessage = builderWithIdentifierFields()
                .setType(Paxos.Message.Type.EP_READ_)
                .setEpRead(epRead_)
                .build();

        Paxos.BebBroadcast bebBroadcast = Paxos.BebBroadcast.newBuilder()
                .setMessage(innerMessage)
                .build();

        Paxos.Message outerMessage = builderWithIdentifierFields()
                .setType(Paxos.Message.Type.BEB_BROADCAST)
                .setBebBroadcast(bebBroadcast)
                .build();

        system.addMessageToQueue(outerMessage);
    }

    protected void broadcastEpWrite() {
        Paxos.EpWrite_ epWrite_ = Paxos.EpWrite_
                .newBuilder()
                .setValue(tmpval)
                .build();

        Paxos.Message innerMessage = builderWithIdentifierFields()
                .setType(Paxos.Message.Type.EP_WRITE_)
                .setEpWrite(epWrite_)
                .build();

        Paxos.BebBroadcast bebBroadcast = Paxos.BebBroadcast
                .newBuilder()
                .setMessage(innerMessage)
                .build();

        Paxos.Message outerMessage = builderWithIdentifierFields()
                .setType(Paxos.Message.Type.BEB_BROADCAST)
                .setBebBroadcast(bebBroadcast)
                .build();

        system.addMessageToQueue(outerMessage);
    }

    private void broadcastEpDecided() {
        Paxos.EpDecided_ epDecided_ = Paxos.EpDecided_
                .newBuilder()
                .setValue(tmpval)
                .build();

        Paxos.Message innerMessage = builderWithIdentifierFields()
                .setType(Paxos.Message.Type.EP_DECIDED_)
                .setEpDecided(epDecided_)
                .build();

        Paxos.BebBroadcast bebBroadcast = Paxos.BebBroadcast
                .newBuilder()
                .setMessage(innerMessage)
                .build();

        Paxos.Message outerMessage = builderWithIdentifierFields()
                .setType(Paxos.Message.Type.BEB_BROADCAST)
                .setBebBroadcast(bebBroadcast)
                .build();

        system.addMessageToQueue(outerMessage);
    }

    private void sendEpState(Paxos.ProcessId destination) {
        Paxos.EpState_ epState_ = Paxos.EpState_
                .newBuilder()
                .setValue(state.val)
                .setValueTimestamp(state.valts)
                .build();

        Paxos.Message innerMessage = builderWithIdentifierFields()
                .setType(Paxos.Message.Type.EP_STATE_)
                .setEpState(epState_)
                .build();

        Paxos.PlSend plSend = Paxos.PlSend
                .newBuilder()
                .setMessage(innerMessage)
                .setDestination(destination)
                .build();

        Paxos.Message outerMessage = builderWithIdentifierFields()
                .setType(Paxos.Message.Type.PL_SEND)
                .setPlSend(plSend)
                .build();

        system.addMessageToQueue(outerMessage);
    }

    private void sendEpAccept(Paxos.ProcessId destination) {
        Paxos.EpAccept_ epAccept_ = Paxos.EpAccept_
                .newBuilder()
                .build();

        Paxos.Message innerMessage = builderWithIdentifierFields()
                .setType(Paxos.Message.Type.EP_ACCEPT_)
                .setEpAccept(epAccept_)
                .build();

        Paxos.PlSend plSend = Paxos.PlSend
                .newBuilder()
                .setMessage(innerMessage)
                .setDestination(destination)
                .build();

        Paxos.Message outerMessage = builderWithIdentifierFields()
                .setType(Paxos.Message.Type.PL_SEND)
                .setPlSend(plSend)
                .build();

        system.addMessageToQueue(outerMessage);
    }

    /**
     * INDICATIONS
     */
    private void triggerEpDecideIndication(Paxos.Value value) {
        Paxos.EpDecide epDecide = Paxos.EpDecide
                .newBuilder()
                .setValue(value)
                .setEts(ets)
                .build();

        Paxos.Message outerMessage = builderWithIdentifierFields()
                .setType(Paxos.Message.Type.EP_DECIDE)
                .setEpDecide(epDecide)
                .build();

        system.addMessageToQueue(outerMessage);
    }

    private void triggerEpAbortedIndication() {
        Paxos.EpAborted epAborted = Paxos.EpAborted
                .newBuilder()
                .setValue(state.val)
                .setValueTimestamp(state.valts)
                .setEts(ets)
                .build();

        Paxos.Message outerMessage = builderWithIdentifierFields()
                .setType(Paxos.Message.Type.EP_ABORTED)
                .setEpAborted(epAborted)
                .build();

        system.addMessageToQueue(outerMessage);
    }

    /**
     * UTILITY
     */
    private EpochConsensusState highestEpochState() {
        List<EpochConsensusState> sortedStates = new ArrayList<>(states.values());
        sortedStates.sort((s1, s2) -> s2.valts - s1.valts);
        return sortedStates.get(0);
    }
}
