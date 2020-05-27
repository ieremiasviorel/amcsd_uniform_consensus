package main.handlers;

import main.Paxos;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class TimerHandler {
    private final List<Paxos.Message> messageQueue;
    private final Timer timer;


    public TimerHandler(List<Paxos.Message> messageQueue) {
        this.messageQueue = messageQueue;
        timer = new Timer();
    }

    public void setTimer(int time, Paxos.Message.Type type) {
        timer.schedule(
                new TimerTask() {
                    @Override
                    public void run() {
                        Paxos.Message message = buildTimerMessage(type);
                        messageQueue.add(message);
                    }
                },
                time
        );

    }

    private Paxos.Message buildTimerMessage(Paxos.Message.Type type) {
        Paxos.Message outerMessage = null;

        switch (type) {
            case EPFD_TIMEOUT:
                Paxos.EpfdTimeout epfdTimeout = Paxos.EpfdTimeout
                        .newBuilder()
                        .build();

                outerMessage = Paxos.Message
                        .newBuilder()
                        .setType(Paxos.Message.Type.EPFD_TIMEOUT)
                        .setEpfdTimeout(epfdTimeout)
                        .build();
                break;
        }

        return outerMessage;
    }
}
