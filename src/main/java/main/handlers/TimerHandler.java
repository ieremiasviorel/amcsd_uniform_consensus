package main.handlers;

import main.ConsensusSystem;
import main.Paxos;

import java.util.Timer;
import java.util.TimerTask;

public class TimerHandler {
    private final Timer timer;


    public TimerHandler() {
        timer = new Timer();
    }

    public void setTimer(int time, Paxos.Message.Type type) {
        timer.schedule(
                new TimerTask() {
                    @Override
                    public void run() {
                        Paxos.Message message = buildTimerMessage(type);
                        ConsensusSystem.getInstance().addMessageToQueue(message);
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
