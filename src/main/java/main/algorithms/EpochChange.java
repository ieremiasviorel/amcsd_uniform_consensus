package main.algorithms;

import main.Paxos;

/**
 * Abstraction: EpochChange
 * Implementation: LeaderBasedEpochChange
 */
public class EpochChange extends AbstractAlgorithm implements Algorithm {

    Paxos.ProcessId trusted;
    int lastts;
    int ts;

    public EpochChange() {
        super("ec");
        trusted = system.getMaxRank(system.getProcesses());
        lastts = 0;
        ts = system.getCurrentProcess().getRank();
    }

    @Override
    public boolean canHandle(Paxos.Message message) {
        /**
         * 1. ELD_TRUST
         * 2. EC_NEW_EPOCH wrapped in BEB_DELIVER
         * 3. EC_NACK wrapped in PL_DELIVER
         */
        return message.getType() == Paxos.Message.Type.ELD_TRUST
                || (message.getType() == Paxos.Message.Type.PL_DELIVER && message.getPlDeliver().getMessage().getType() == Paxos.Message.Type.EC_NEW_EPOCH_)
                || (message.getType() == Paxos.Message.Type.PL_DELIVER && message.getPlDeliver().getMessage().getType() == Paxos.Message.Type.EC_NACK_);
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
            case ELD_TRUST:
                handleEldTrust(message);
                break;
            case EC_NEW_EPOCH_:
                handleEcNewEpoch(message);
                break;
            case EC_NACK_:
                handleEcNack(message);
                break;
        }
    }

    private void handleEldTrust(Paxos.Message message) {
        Paxos.ProcessId eldTrusted = message.getEldTrust().getProcess();
        trusted = eldTrusted;

        if (system.getCurrentProcess().equals(eldTrusted)) {
            ts += system.getProcesses().size();
            broadcastEcNewEpoch(ts);
        }
    }

    private void handleEcNewEpoch(Paxos.Message message) {
        Paxos.PlDeliver plDeliver = message.getPlDeliver();
        Paxos.ProcessId sender = plDeliver.getSender();
        Paxos.EcNewEpoch_ ecNewEpoch_ = plDeliver.getMessage().getEcNewEpoch();
        int newts = ecNewEpoch_.getTimestamp();

        if (sender.equals(trusted) && newts > lastts) {
            lastts = newts;
            triggerEcStartEpochIndication(sender, newts);
        } else {
            sendEcNack(sender);
        }
    }

    private void handleEcNack(Paxos.Message message) {
        ts += system.getProcesses().size();
        broadcastEcNewEpoch(ts);
    }

    private void triggerEcStartEpochIndication(Paxos.ProcessId newLeader, int newts) {
        Paxos.EcStartEpoch ecStartEpoch = Paxos.EcStartEpoch
                .newBuilder()
                .setNewLeader(newLeader)
                .setNewTimestamp(newts)
                .build();

        Paxos.Message outerMessage = builderWithIdentifierFields()
                .setType(Paxos.Message.Type.EC_START_EPOCH)
                .setEcStartEpoch(ecStartEpoch)
                .build();

        system.addMessageToQueue(outerMessage);
    }

    private void broadcastEcNewEpoch(int ts) {
        Paxos.EcNewEpoch_ newEpoch_ = Paxos.EcNewEpoch_
                .newBuilder()
                .setTimestamp(ts)
                .build();

        Paxos.Message innerMessage = builderWithIdentifierFields()
                .setType(Paxos.Message.Type.EC_NEW_EPOCH_)
                .setEcNewEpoch(newEpoch_)
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

    private void sendEcNack(Paxos.ProcessId sender) {
        Paxos.EcNack_ ecNack_ = Paxos.EcNack_
                .newBuilder()
                .build();

        Paxos.Message innerMessage = builderWithIdentifierFields()
                .setType(Paxos.Message.Type.EC_NACK_)
                .setEcNack(ecNack_)
                .build();

        Paxos.PlSend plSend = Paxos.PlSend
                .newBuilder()
                .setMessage(innerMessage)
                .setDestination(sender)
                .build();

        Paxos.Message outerMessage = builderWithIdentifierFields()
                .setType(Paxos.Message.Type.PL_SEND)
                .setPlSend(plSend)
                .build();

        system.addMessageToQueue(outerMessage);
    }
}
