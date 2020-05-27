package main;

import main.algorithms.Algorithm;
import main.algorithms.Application;
import main.handlers.EventLoop;
import main.handlers.NetworkHandler;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static main.Main.*;

public class ConsensusSystem {

    private final List<Paxos.Message> messageQueue;
    private final List<Algorithm> algorithms;
    private final int processPort;

    private static List<Paxos.ProcessId> processList;

    public ConsensusSystem(int processPort) {
        this.messageQueue = Collections.synchronizedList(new ArrayList<>());
        this.algorithms = new ArrayList<>();
        this.processPort = processPort;
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
        EventLoop eventLoop = new EventLoop(messageQueue, algorithms);
        eventLoop.start();

        /**
         * Instantiate and start the {@link NetworkHandler}
         */
        NetworkHandler server = new NetworkHandler(processPort, messageQueue);
        server.start();

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

        Paxos.NetworkMessage networkMessage = Paxos.NetworkMessage
                .newBuilder()
                .setMessage(innerMessage)
                .setSenderHost(PROCESS_HOST)
                .setSenderListeningPort(processPort)
                .build();

        Paxos.Message outerMessage = Paxos.Message
                .newBuilder()
                .setType(Paxos.Message.Type.NETWORK_MESSAGE)
                .setNetworkMessage(networkMessage)
                .build();

        byte[] encodedMessage = outerMessage.toByteArray();

        Socket hubSocket = new Socket(HUB_HOST, HUB_PORT);
        DataOutputStream dOut = new DataOutputStream(hubSocket.getOutputStream());

        dOut.writeInt(encodedMessage.length);
        dOut.write(encodedMessage);

        dOut.close();
        hubSocket.close();
    }

    private int getProcessIndex(int processPort) {
        return (processPort % 5000) - 3;
    }

    public static List<Paxos.ProcessId> getProcessList() {
        return processList;
    }

    public static void setProcessList(List<Paxos.ProcessId> processList) {
        ConsensusSystem.processList = processList;
    }
}
