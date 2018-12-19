package test;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

/**
 * @author WangPingChun
 * @since 2018-12-19
 */

@Slf4j
public class ProducerTest {
    private Producer<String, String> producer;

    @Before
    public void before() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.211.55.4:9092");
        // acks:producer需要server接收到数据之后发出的确认接收的信号，此项配置就是指producer需要多少个这样的确认信号
        // acks = 0 不需要等待任何确认收到的信息，副本将立即添加到socket buffer并认为已经发送。没有任何保障可以保证此种情况下server已经
        //          成功接收数据，同时重试配置不会作用（因为客户端不知道是否失败）回馈的offset会总是设置为-1
        // acks = 1 这就意味着至少要等待leader已经成功将数据写入本地log，但是并没有等待所有follower是否成功写入。这种情况下，如果follower
        //          没有成功备份数据，而此时leader又挂掉，则消息会丢失
        // acks = all 这意味着leader需要等待所有备份都成功写入日志，这种策略会保证只要有一个备份存活就不会丢失数据，这是最强的保证
        props.put("acks", "1");
        props.put("retries", 0);
        // batch.size:缓存每个分区未发送的消息，值较大的话将会产生更大的批，并需要更多的内存（每个「活跃」的分区都有一个缓冲区）
        props.put("batch.size", 16384);
        // linger.ms:默认缓存可立即发送，即使缓存空间还没有满，如果想减少请求的数量，可以设置linger.ms大于0
        // 该参数将指示生产者发送请求之前等待一段时间，希望更多的消息填补到未满的批中
        props.put("linger.ms", 1);
        // buffer.memory:控制生产者可用的缓存总量。如果消息发送速度比其传输到服务器的快，将会耗尽这个缓存空间
        // 当缓存空间耗尽，其它发送调用将被阻塞，阻塞时间的阀值通过max.block.ms设定，之后它将抛出一个TimeoutException
        props.put("buffer.memory", 33554432);
        // 将用户提供的key和value对象ProducerRecord转换成字节
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<>(props);
    }

    @Test
    public void testProducer() throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(100);
        final long startTime = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<>("my-topic", String.valueOf(i), String.valueOf(i)), (metadata, exception) -> {
                log.info("metadata \n {}", metadata);
                countDownLatch.countDown();
            });
        }
        countDownLatch.await();
        log.info("send finished! elapsed time: {}ms", (System.currentTimeMillis() - startTime));
        producer.close();
    }

    @Test
    public void testProducerBlock() throws ExecutionException, InterruptedException {
        ProducerRecord<String, String> record = new ProducerRecord<>("my-topic", "key", "value");

        final RecordMetadata recordMetadata = producer.send(record).get();
        log.info("recordMetadata ===> offset:{},partition:{},serializedKeySize:{},serializedValueSize:{},timestamp:{},topic:{}",
                recordMetadata.offset(),
                recordMetadata.partition(),
                recordMetadata.serializedKeySize(),
                recordMetadata.serializedValueSize(),
                recordMetadata.timestamp(),
                recordMetadata.topic());
    }

    @Test
    public void testSendAsync() throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        ProducerRecord<String, String> record = new ProducerRecord<>("my-topic", "key", "value");
        /*
            send会出现的异常：
            InterruptException - 如果线程在阻塞中断
            SerializationException - 如果key或value不是给定有效配置的serializers
            TimeoutException - 如果获取元数据或消息分配内存话费的时间超过max.block.ms
            KafkaException - Kafka有关的错误
         */
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("send error", exception);
            }
            log.info("recordMetadata ===> \n offset : {},\n partition : {},\n serializedKeySize : {},\n serializedValueSize : {},\n timestamp :{},topic : {} \n",
                    metadata.offset(),
                    metadata.partition(),
                    metadata.serializedKeySize(),
                    metadata.serializedValueSize(),
                    metadata.timestamp(),
                    metadata.topic());
            countDownLatch.countDown();
        });
        countDownLatch.await();
    }
}
