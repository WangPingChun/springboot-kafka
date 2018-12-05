package wang.crispin.lean.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import wang.crispin.lean.kafka.provider.KafkaSender;

/**
 * @author WangPingChun
 */
@SpringBootApplication
public class KafkaApplication {

    public static void main(String[] args) {
        final ConfigurableApplicationContext context = SpringApplication.run(KafkaApplication.class, args);
        final KafkaSender kafkaSender = context.getBean(KafkaSender.class);
        kafkaSender.send();
    }
}
