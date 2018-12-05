package wang.crispin.lean.kafka.beans;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Date;

/**
 * @author WangPingChun
 * @since 2018-12-04
 */
@Getter
@Setter
@ToString
public class Message {
    private Long id;
    private String msg;
    private Date sendTime;
}
