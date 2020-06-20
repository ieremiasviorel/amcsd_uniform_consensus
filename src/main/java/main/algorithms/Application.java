package main.algorithms;

import main.Main;
import main.Paxos;

import java.io.IOException;
import java.util.HashSet;

import static main.Main.HUB_HOST;
import static main.Main.HUB_PORT;

public class Application extends AbstractAlgorithm implements Algorithm {

    public Application() {
        super("app");
    }

    @Override
    public boolean canHandle(Paxos.Message message) {
        /**
         * 1. APP_PROPOSE wrapped in a PL_DELIVER
         */
        return (message.getType() == Paxos.Message.Type.PL_DELIVER &&
                message.getPlDeliver().getMessage().getType() == Paxos.Message.Type.APP_PROPOSE)
                || message.getType() == Paxos.Message.Type.UC_DECIDE;
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
            case APP_PROPOSE:
                handleAppPropose(message);
                break;
            case UC_DECIDE:
                handleUcDecide(message);
                break;
        }
    }

    private void handleAppPropose(Paxos.Message message) {
        Paxos.AppPropose appPropose = message.getPlDeliver().getMessage().getAppPropose();

        system.setProcesses(new HashSet<>(appPropose.getProcessesList()));
        system.initializeDefaultAlgorithms();

        Paxos.UcPropose ucPropose = Paxos.UcPropose
                .newBuilder()
                .setValue(appPropose.getValue())
                .build();

        Paxos.Message outerMessage = builderWithIdentifierFields()
                .setType(Paxos.Message.Type.UC_PROPOSE)
                .setUcPropose(ucPropose)
                .build();

        system.addMessageToQueue(outerMessage);
    }

    private void handleUcDecide(Paxos.Message message) {
        Paxos.UcDecide ucDecide = message.getUcDecide();
        Paxos.Value value = ucDecide.getValue();

        System.out.println(Main.ANSI_WHITE + "UC DECIDE " + value.getV() + Main.ANSI_RESET);

        Paxos.AppDecide appDecide = Paxos.AppDecide
                .newBuilder()
                .setValue(value)
                .build();

        Paxos.Message outerMessage = builderWithIdentifierFields()
                .setType(Paxos.Message.Type.APP_DECIDE)
                .setAppDecide(appDecide)
                .build();

        try {
            system.sendMessageOverTheNetwork(outerMessage, HUB_HOST, HUB_PORT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
