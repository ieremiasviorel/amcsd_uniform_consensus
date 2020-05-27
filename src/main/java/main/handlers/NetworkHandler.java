package main.handlers;

import main.Paxos;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

public class NetworkHandler extends Thread {

    private final int port;
    private final List<Paxos.Message> messageQueue;
    private final ServerSocket serverSocket;

    private boolean acceptClientRequests;

    public NetworkHandler(int port, List<Paxos.Message> messageQueue) throws IOException {
        this.port = port;
        this.messageQueue = messageQueue;
        this.serverSocket = new ServerSocket(port);
        this.acceptClientRequests = true;
    }

    public void sendMessage(Paxos.Message message) {

    }

    @Override
    public void run() {
        try {
            openServerSocket();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void openServerSocket() throws IOException {
        System.out.println("Waiting for requests on port " + port);

        while (acceptClientRequests) {
            Socket clientSocket = serverSocket.accept();
            NetworkClientHandler clientHandler = new NetworkClientHandler(clientSocket, messageQueue);
            clientHandler.start();
        }
    }

    public void closeServerSocket() throws IOException {
        acceptClientRequests = false;
        serverSocket.close();
    }
}
