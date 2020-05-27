package main.handlers;

import main.Paxos;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Arrays;

public class NetworkClientHandler extends Thread {

    private final Socket clientSocket;
    private final DataInputStream dIn;

    public NetworkClientHandler(Socket socket) throws IOException {
        clientSocket = socket;
        dIn = new DataInputStream(clientSocket.getInputStream());
    }

    public void run() {
        try {
            int messageSize = dIn.readInt();
            byte[] byteBuffer = new byte[messageSize];
            dIn.read(byteBuffer, 0, messageSize);
            Paxos.Message outerMessage = Paxos.Message.parseFrom(byteBuffer);
            System.out.println("Outer message type: " + outerMessage.getType());
            Paxos.NetworkMessage networkMessage = outerMessage.getNetworkMessage();
            Paxos.Message innerMessage = networkMessage.getMessage();
            System.out.println("Inner message type: " + innerMessage.getType());
            Paxos.AppPropose appProposeMessage = innerMessage.getAppPropose();
            System.out.println(Arrays.toString(appProposeMessage.getProcessesList().toArray()));
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            this.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void close() throws IOException {
        dIn.close();
        clientSocket.close();
    }
}
