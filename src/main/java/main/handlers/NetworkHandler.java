package main.handlers;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class NetworkHandler extends Thread {

    private final int port;
    private final ServerSocket serverSocket;
    private boolean acceptClientRequests;

    public NetworkHandler(int port) throws IOException {
        this.port = port;
        this.serverSocket = new ServerSocket(port);
        this.acceptClientRequests = true;
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
            NetworkClientHandler clientHandler = new NetworkClientHandler(clientSocket);
            clientHandler.start();
            this.acceptClientRequests = false;
        }
    }

    public void closeServerSocket() throws IOException {
        acceptClientRequests = false;
        serverSocket.close();
    }
}
