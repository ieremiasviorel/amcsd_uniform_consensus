package main;

import java.io.IOException;

public class Main {

    public static final String HUB_HOST = "127.0.0.1";
    public static final int HUB_PORT = 5000;
    public static final String PROCESS_HOST = "127.0.0.1";

    public static void main(String[] args) throws IOException {

        int processPort = Integer.parseInt(args[0]);

        ConsensusSystem consensusSystem = new ConsensusSystem(processPort);

        consensusSystem.start();
    }
}
