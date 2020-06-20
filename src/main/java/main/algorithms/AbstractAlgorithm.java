package main.algorithms;

import main.ConsensusSystem;
import main.Paxos;

import java.util.UUID;

public abstract class AbstractAlgorithm {

    protected final ConsensusSystem system;
    protected final String abstractionId;

    protected AbstractAlgorithm(String abstractionId) {
        this.system = ConsensusSystem.getInstance();
        this.abstractionId = abstractionId;
    }

    protected Paxos.Message.Builder builderWithIdentifierFields() {
        return Paxos.Message
                .newBuilder()
                .setSystemId(system.getSystemId())
                .setAbstractionId(abstractionId)
                .setMessageUuid(UUID.randomUUID().toString());
    }
}
