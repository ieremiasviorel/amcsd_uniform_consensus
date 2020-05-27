package main;

import main.algorithms.Algorithm;
import main.algorithms.Application;
import main.algorithms.EventuallyPerfectFailureDetector;
import main.algorithms.PerfectLink;
import main.handlers.EventLoop;
import main.handlers.NetworkHandler;
import main.handlers.TimerHandler;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import static main.Main.HUB_HOST;
import static main.Main.HUB_PORT;

public class ConsensusSystem {

    private static ConsensusSystem instance;

    private final List<Paxos.Message> messageQueue;
    private final List<Algorithm> algorithms;

    private Set<Paxos.ProcessId> processes;
    private Paxos.ProcessId currentProcess;
    private int processPort;
    private int processIndex;

    private final String systemId;
    private final String owner;

    private EventLoop eventLoop;
    private NetworkHandler networkHandler;
    private TimerHandler timerHandler;

    public static ConsensusSystem getInstance() {
        if (instance == null)
            instance = new ConsensusSystem();

        return instance;
    }

    private ConsensusSystem() {
        this.messageQueue = new CopyOnWriteArrayList<>();
        this.algorithms = new CopyOnWriteArrayList<>();
        this.processes = new HashSet<>();
        this.systemId = "sys-1";
        this.owner = "iv";
    }

    public void start() throws IOException {
        /**
         * Instantiate the {@link Application} algorithm and add it to the algorithms list
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
         * Instantiate the {@link TimerHandler}
         */
        timerHandler = new TimerHandler(messageQueue);

        /**
         * Register the node with the HUB
         */
        registerToHub();
    }

    private void registerToHub() throws IOException {
        Paxos.AppRegistration appRegistration = Paxos.AppRegistration
                .newBuilder()
                .setOwner(owner)
                .setIndex(processIndex)
                .build();

        Paxos.Message innerMessage = Paxos.Message
                .newBuilder()
                .setType(Paxos.Message.Type.APP_REGISTRATION)
                .setAppRegistration(appRegistration)
                .setSystemId(systemId)
                .setAbstractionId("app")
                .setMessageUuid(UUID.randomUUID().toString())
                .build();

        this.sendMessageOverTheNetwork(innerMessage, HUB_HOST, HUB_PORT);
    }

    public void initializeDefaultAlgorithms() {
        algorithms.add(new PerfectLink());
        algorithms.add(new EventuallyPerfectFailureDetector());
    }

    public Paxos.ProcessId getProcessIdByPort(int port) {
        Optional<Paxos.ProcessId> processIdOptional = processes
                .stream()
                .filter(processId -> processId.getPort() == port)
                .findFirst();

        return processIdOptional.orElse(null);
    }

    public Set<Paxos.ProcessId> getProcesses() {
        return processes;
    }

    public void sendMessageOverTheNetwork(Paxos.Message message, String host, int port) throws IOException {
        this.networkHandler.sendMessage(message, host, port);
    }

    public void addMessageToQueue(Paxos.Message message) {
        this.messageQueue.add(message);
    }

    public void setTimer(int time, Paxos.Message.Type type) {
        this.timerHandler.setTimer(time, type);
    }

    public void setProcesses(Set<Paxos.ProcessId> processes) {
        this.processes = processes;
        this.currentProcess = getProcessIdByPort(processPort);
    }

    public void setProcessPort(int processPort) {
        this.processPort = processPort;
    }

    public void setProcessIndex(int processIndex) {
        this.processIndex = processIndex;
    }

    public String getSystemId() {
        return systemId;
    }

    public Paxos.ProcessId getMaxRank(Set<Paxos.ProcessId> processes) {
        List<Paxos.ProcessId> processesList = new ArrayList<>(processes);

        processesList.sort(new Comparator<Paxos.ProcessId>() {
            @Override
            public int compare(Paxos.ProcessId o1, Paxos.ProcessId o2) {
                return o2.getRank() - o1.getRank();
            }
        });

        return processesList.get(0);
    }
}
