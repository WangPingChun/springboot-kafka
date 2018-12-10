package group;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

/**
 * @author WangPingChun
 * @since 2018-12-10
 */
public class ConsumerTest implements Runnable {
    private KafkaStream kafkaStream;
    private int threadNumber;

    public ConsumerTest(KafkaStream<byte[], byte[]> stream, int threadNumber) {
        this.kafkaStream = stream;
        this.threadNumber = threadNumber;
    }

    @Override
    public void run() {
        final ConsumerIterator iterator = kafkaStream.iterator();
        while (iterator.hasNext()) {
            System.out.println("Thread " + threadNumber + ": " + new String(iterator.next().message()));
        }
    }
}
