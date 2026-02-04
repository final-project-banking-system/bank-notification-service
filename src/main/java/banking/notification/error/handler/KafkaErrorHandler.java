package banking.notification.error.handler;

import com.fasterxml.jackson.core.JsonParseException;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

@Configuration
public class KafkaErrorHandler extends DefaultErrorHandler {
    public KafkaErrorHandler(KafkaTemplate<String, String> kafkaTemplate,
                             @Value("${spring.kafka.consumer.group-id}")
                             String consumerGroupId) {
        super(
                new DeadLetterPublishingRecoverer(
                        kafkaTemplate,
                        (record, ex) -> new TopicPartition(
                                record.topic() + "." + consumerGroupId + ".dq", record.partition())
                ),
                new ExponentialBackOffWithMaxRetries(3)
        );
        addNotRetryableExceptions(
                JsonParseException.class,
                IllegalArgumentException.class
        );
        setCommitRecovered(true);
    }
}
