package main.algorithms;

import main.ConsensusSystem;
import main.Paxos;

import java.util.UUID;

public abstract class AbstractAlgorithm {

    protected final ConsensusSystem system;
    protected final String abstractionId;

    protected AbstractAlgorithm() {
        system = ConsensusSystem.getInstance();
        abstractionId = getAbstractionId();
    }

    abstract String getAbstractionId();

    protected Paxos.Message.Builder builderWithIdentifierFields() {
        return Paxos.Message
                .newBuilder()
                .setSystemId(system.getSystemId())
                .setAbstractionId(abstractionId)
                .setMessageUuid(UUID.randomUUID().toString());
    }
}
