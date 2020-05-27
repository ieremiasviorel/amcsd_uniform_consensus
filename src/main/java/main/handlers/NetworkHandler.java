package main.handlers;

import main.Paxos;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

import static main.Main.*;

public class NetworkHandler extends Thread {

    private final int processPort;
    private final List<Paxos.Message> messageQueue;
    private final ServerSocket serverSocket;

    private boolean acceptClientRequests;

    public NetworkHandler(int processPort, List<Paxos.Message> messageQueue) throws IOException {
        this.processPort = processPort;
        this.messageQueue = messageQueue;
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
                .setSystemId(message.getSystemId())
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
                    new IncomingMessageHandler(clientSocket, messageQueue);
            incomingMessageHandler.start();
        }
    }
}
