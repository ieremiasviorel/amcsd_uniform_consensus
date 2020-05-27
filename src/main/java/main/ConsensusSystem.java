package main;

import main.algorithms.Algorithm;
import main.algorithms.Application;
import main.algorithms.EventuallyPerfectFailureDetector;
import main.algorithms.PerfectLink;
import main.handlers.EventLoop;
import main.handlers.NetworkHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

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
    private String systemId;

    public static ConsensusSystem getInstance() {
        if (instance == null)
            instance = new ConsensusSystem();

        return instance;
    }

    private ConsensusSystem() {
        this.messageQueue = new CopyOnWriteArrayList<>();
        this.algorithms = new CopyOnWriteArrayList<>();
        this.processList = new ArrayList<>();
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
                .setMessageUuid("")
                .setSystemId("sys-1")
                .build();

        this.sendMessage(innerMessage, HUB_HOST, HUB_PORT);
    }

    public void initializeDefaultAlgorithms() {
        algorithms.add(new PerfectLink());
        algorithms.add(new EventuallyPerfectFailureDetector());
    }

    public void sendMessage(Paxos.Message message, String receiverHost, int receiverPort) throws IOException {
        this.networkHandler.sendMessage(message, receiverHost, receiverPort);
    }

    public Paxos.ProcessId getProcessIdByPort(int port) {
        Optional<Paxos.ProcessId> processIdOptional = processList
                .stream()
                .filter(processId -> processId.getPort() == port)
                .findFirst();

        return processIdOptional.orElse(null);
    }

    public List<Paxos.ProcessId> getProcessList() {
        return processList;
    }

    public void setProcessList(List<Paxos.ProcessId> processList) {
        this.processList = processList;
    }

    public void setProcessPort(int processPort) {
        this.processPort = processPort;
    }

    public NetworkHandler getNetworkHandler() {
        return networkHandler;
    }

    public List<Paxos.Message> getMessageQueue() {
        return messageQueue;
    }

    public int getProcessIndex(int processPort) {
        return (processPort % 5000) - 3;
    }

    public String getSystemId() {
        return systemId;
    }

    public void setSystemId(String systemId) {
        this.systemId = systemId;
    }
}
