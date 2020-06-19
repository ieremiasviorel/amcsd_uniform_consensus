package main;

import java.io.IOException;

public class Main {

    public static final String HUB_HOST = "127.0.0.1";
    public static final int HUB_PORT = 5000;
    public static final String PROCESS_HOST = "127.0.0.1";

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_BLACK = "\u001B[30m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_PURPLE = "\u001B[35m";
    public static final String ANSI_CYAN = "\u001B[36m";
    public static final String ANSI_WHITE = "\u001B[37m";

    public static void main(String[] args) throws IOException {

        int processPort = Integer.parseInt(args[0]);

        ConsensusSystem consensusSystem = ConsensusSystem.getInstance();
        consensusSystem.setProcessPort(processPort);
        consensusSystem.setProcessIndex(processPort % 5000 - 3);

        consensusSystem.start();
    }

    public static String getPrintableMessageType(Paxos.Message message) {
        if (message.getType() == Paxos.Message.Type.PL_DELIVER) {
            return "PL_DELIVER[" + message.getPlDeliver().getMessage().getType() + "]";
        } else if (message.getType() == Paxos.Message.Type.PL_SEND) {
            return "PL_SEND[" + message.getPlSend().getMessage().getType() + "]";
        } else {
            return message.getType().toString();
        }
    }
}
