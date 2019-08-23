package ai.kudi.fridaytalk.blockingqueue;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class BlockingQueueExample {
    public static void main(String[] args) {
        final int BOUND = 10;
        final int N_PRODUCERS = 4;
        final int N_CONSUMERS = Runtime.getRuntime().availableProcessors();
        final int poisonPill = Integer.MAX_VALUE;
        final int poisonPillPerProducer = N_CONSUMERS / N_PRODUCERS;

        final BlockingQueue<Integer> queue = new LinkedBlockingQueue<>(BOUND);

        for (int i = 0; i < N_PRODUCERS; i++) {
            new Thread(new NumbersProducer(queue, poisonPill, poisonPillPerProducer)).start();
        }

        for (int j = 0; j < N_CONSUMERS; j++) {
            new Thread(new NumbersConsumer(queue, poisonPill)).start();
        }
    }
}