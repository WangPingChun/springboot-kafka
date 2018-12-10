package group;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author WangPingChun
 * @since 2018-12-10
 */
public class GroupConsumerTest extends Thread {
    private final ConsumerConnector consumerConnector;
    private final String topic;
    private ExecutorService executorService;

    public GroupConsumerTest(String zookeeper, String groupId, String topic) {
        consumerConnector = Consumer.createJavaConsumerConnector(createConsumerConfig(zookeeper, groupId));
        this.topic = topic;
    }

    private ConsumerConfig createConsumerConfig(String zookeeper, String groupId) {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", zookeeper);
        properties.put("group.id", groupId);
        properties.put("zookeeper.session.timeout.ms", "40000");
        properties.put("zookeeper.sync.time.ms", "2000");
        properties.put("auto.commit.interval.ms", "1000");

        return new ConsumerConfig(properties);
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Please assign partition number.");
        }

        String zookeeper = "10.211.55.4:2181,10.211.55.7:2181,10.211.55.8:2181";
        String groupId = "jikegrouptest";
        String topic = "jiketest";
        int threads = Integer.parseInt(args[0]);

        GroupConsumerTest example = new GroupConsumerTest(zookeeper, groupId, topic);
        example.run(threads);
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {

        }
        example.shutdown();
    }

    private void run(int threads) {
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, new Integer(threads));
        final Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
        final List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        executorService = Executors.newFixedThreadPool(threads);

        int threadNumber = 0;
        for (KafkaStream<byte[], byte[]> stream : streams) {
            executorService.submit(new ConsumerTest(stream, threadNumber));
            threadNumber++;
        }

    }

    private void shutdown() {
        if (consumerConnector != null) {
            consumerConnector.shutdown();
        }
        if (executorService != null) {
            executorService.shutdown();
        }
        try {
            if (!executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS)) {
                System.out.println("Timed out waiting for consumer threads to shutdown, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            System.out.println("Interrupted during shutdown,exiting uncleanly");
        }
    }
}
