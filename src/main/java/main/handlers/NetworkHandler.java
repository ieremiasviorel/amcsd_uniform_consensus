package main.handlers;

import main.Paxos;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import static main.Main.PROCESS_HOST;

public class NetworkHandler extends Thread {

    private final int processPort;
    private final ServerSocket serverSocket;

    private boolean acceptClientRequests;

    public NetworkHandler(int processPort) throws IOException {
        this.processPort = processPort;
        this.serverSocket = new ServerSocket(processPort);
        this.acceptClientRequests = true;
    }

    public void sendMessage(Paxos.Message message, String receiverHost, int receiverPort) throws IOException {
        Paxos.NetworkMessage networkMessage = Paxos.NetworkMessage
                .newBuilder()
                .setMessage(message)
                .setSenderHost(PROCESS_HOST)
                .setSenderListeningPort(processPort)
                .build();

        Paxos.Message outerMessage = Paxos.Message
                .newBuilder()
                .setSystemId("sys-1")
                .setAbstractionId(message.getAbstractionId())
                .setType(Paxos.Message.Type.NETWORK_MESSAGE)
                .setNetworkMessage(networkMessage)
                .build();

        byte[] encodedMessage = outerMessage.toByteArray();
        Socket sendSocket = new Socket(receiverHost, receiverPort);
        DataOutputStream dOut = new DataOutputStream(sendSocket.getOutputStream());

        dOut.writeInt(encodedMessage.length);
        dOut.write(encodedMessage);

        dOut.close();
        sendSocket.close();
    }

    @Override
    public void run() {
        try {
            openServerSocket();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void closeServerSocket() throws IOException {
        acceptClientRequests = false;
        serverSocket.close();
    }

    private void openServerSocket() throws IOException {
        System.out.println("Waiting for requests on port " + processPort);

        while (acceptClientRequests) {
            Socket clientSocket = serverSocket.accept();
            IncomingMessageHandler incomingMessageHandler =
                    new IncomingMessageHandler(clientSocket);
            incomingMessageHandler.start();
        }
    }
}
