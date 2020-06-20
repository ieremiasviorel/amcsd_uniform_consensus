package main.utils;

import main.Paxos;

import java.util.Objects;

public class EpochConsensusState {

    public final Paxos.Value val;
    public final int valts;

    public EpochConsensusState(Paxos.Value val, int valts) {
        this.val = val;
        this.valts = valts;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EpochConsensusState that = (EpochConsensusState) o;
        return valts == that.valts &&
                Objects.equals(val, that.val);
    }

    @Override
    public int hashCode() {
        return Objects.hash(val, valts);
    }
}
