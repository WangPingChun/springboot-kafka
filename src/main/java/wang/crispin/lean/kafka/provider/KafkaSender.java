package wang.crispin.lean.kafka.provider;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import wang.crispin.lean.kafka.beans.Message;

import java.util.Date;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * @author WangPingChun
 * @since 2018-12-04
 */
@Slf4j
@Component
public class KafkaSender {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    private Gson gson = new GsonBuilder().create();

    /**
     * 发送消息
     */
    public void send() {
        Message message = new Message();
        message.setId(System.currentTimeMillis());
        message.setMsg(UUID.randomUUID().toString());
        message.setSendTime(new Date());
        //log.info("message = {}", message);
        // topic为"test"
        //kafkaTemplate.send("test", gson.toJson(message));
        //try {
        //    final SendResult<String, String> sendResult = kafkaTemplate.send("CustomerCountry", "Precision Products", "France").get();
        //    System.out.println(sendResult);
        //} catch (InterruptedException e) {
        //    e.printStackTrace();
        //} catch (ExecutionException e) {
        //    e.printStackTrace();
        //}
        kafkaTemplate.send("CustomerCountry", "Precision Products", "France")
                .addCallback(successResult -> {
                    log.info(successResult.toString());
                }, failureResult -> {
                    log.info("error", failureResult);
                });
    }
}
