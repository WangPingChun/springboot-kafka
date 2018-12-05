package wang.crispin.lean.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * @author WangPingChun
 * @since 2018-12-04
 */
@Slf4j
@Component
public class KafkaReceiver {
    @KafkaListener(topics = {"CustomerCountry"})
    public void listen(ConsumerRecord<?, ?> record) {
        log.info("record = {}", record);
        final Optional<?> kafkaMessage = Optional.ofNullable(record.value());
        kafkaMessage.ifPresent(message -> {
            log.info("接收到消息 = {}", message);
        });
    }
}
