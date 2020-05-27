package main.handlers;

import main.Paxos;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.List;

public class IncomingMessageHandler extends Thread {

    private final Socket clientSocket;
    private final List<Paxos.Message> messageQueue;
    private final DataInputStream dIn;

    public IncomingMessageHandler(Socket clientSocket, List<Paxos.Message> messageQueue) throws IOException {
        this.clientSocket = clientSocket;
        this.messageQueue = messageQueue;
        this.dIn = new DataInputStream(this.clientSocket.getInputStream());
    }

    @Override
    public void run() {
        try {
            int messageSize = dIn.readInt();
            byte[] byteBuffer = new byte[messageSize];
            int readMessageSize = dIn.read(byteBuffer, 0, messageSize);

            if (messageSize != readMessageSize) {
                throw new RuntimeException("Network has incorrect size: expected = " + messageSize + ", actual = " + readMessageSize);
            }

            Paxos.Message outerMessage = Paxos.Message.parseFrom(byteBuffer);

            if (outerMessage.getType() != Paxos.Message.Type.NETWORK_MESSAGE) {
                throw new RuntimeException("Network message has incorrect type: expected = " + Paxos.Message.Type.NETWORK_MESSAGE + ", actual size " + outerMessage.getType());
            }

            Paxos.NetworkMessage networkMessage = outerMessage.getNetworkMessage();

            Paxos.Message innerMessage = networkMessage.getMessage();

            messageQueue.add(innerMessage);

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
