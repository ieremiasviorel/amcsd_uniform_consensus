package main;

import main.algorithms.Algorithm;
import main.algorithms.Application;
import main.handlers.EventLoop;
import main.handlers.NetworkHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static main.Main.HUB_HOST;
import static main.Main.HUB_PORT;

public class ConsensusSystem {

    private static ConsensusSystem instance;

    private final List<Paxos.Message> messageQueue;
    private final List<Algorithm> algorithms;

    private int processPort;
    private EventLoop eventLoop;
    private NetworkHandler networkHandler;
    private List<Paxos.ProcessId> processList;

    public static ConsensusSystem getInstance() {
        if (instance == null)
            instance = new ConsensusSystem();

        return instance;
    }

    private ConsensusSystem() {
        this.messageQueue = Collections.synchronizedList(new ArrayList<>());
        this.algorithms = new ArrayList<>();
    }

    public void start() throws IOException {
        /**
         * Instantiate the {@link Application} algorithm and
         * add it to the algorithms list
         */
        Application application = new Application();
        algorithms.add(application);

        /**
         * Instantiate and start the {@link EventLoop}
         */
        eventLoop = new EventLoop(messageQueue, algorithms);
        eventLoop.start();

        /**
         * Instantiate and start the {@link NetworkHandler}
         */
        networkHandler = new NetworkHandler(processPort, messageQueue);
        networkHandler.start();

        /**
         * Register the node with the HUB
         */
        registerToHub();
    }

    private void registerToHub() throws IOException {
        Paxos.AppRegistration appRegistration = Paxos.AppRegistration
                .newBuilder()
                .setOwner("IV")
                .setIndex(getProcessIndex(processPort))
                .build();

        Paxos.Message innerMessage = Paxos.Message
                .newBuilder()
                .setType(Paxos.Message.Type.APP_REGISTRATION)
                .setAppRegistration(appRegistration)
                .setAbstractionId("app")
                .setMessageUuid("appRegistration")
                .setSystemId("ref-" + getProcessIndex(processPort))
                .build();

        this.sendMessage(innerMessage, HUB_HOST, HUB_PORT);
    }

    private int getProcessIndex(int processPort) {
        return (processPort % 5000) - 3;
    }

    public List<Paxos.ProcessId> getProcessList() {
        return processList;
    }

    public void setProcessList(List<Paxos.ProcessId> processList) {
        this.processList = processList;
    }

    public void sendMessage(Paxos.Message message, String receiverHost, int receiverPort) throws IOException {
        this.networkHandler.sendMessage(message, receiverHost, receiverPort);
    }


    public int getProcessPort() {
        return processPort;
    }

    public void setProcessPort(int processPort) {
        this.processPort = processPort;
    }
}
