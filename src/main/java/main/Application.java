package main;

import main.handlers.NetworkHandler;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class Application {

    public static final String HUB_HOST = "127.0.0.1";
    public static final int HUB_PORT = 5000;
    public static final String PROCESS_HOST = "127.0.0.1";

    public static void main(String[] args) throws IOException {
        int processPort = Integer.parseInt(args[0]);

        NetworkHandler server = new NetworkHandler(processPort);
        server.start();

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

    private static int getProcessIndex(int processPort) {
        return (processPort % 5000) - 3;
    }
}
