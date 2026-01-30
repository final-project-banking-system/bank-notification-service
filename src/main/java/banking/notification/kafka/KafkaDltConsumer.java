package banking.notification.kafka;

import banking.notification.dto.kafka.KafkaEventWrapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Component
@RequiredArgsConstructor
@Transactional
public class KafkaDltConsumer {

    private final ObjectMapper objectMapper;

    @DltHandler
    public void handleDltMessage(ConsumerRecord<String, Object> record,
                                 @Payload(required = false) KafkaEventWrapper wrapper,
                                 Acknowledgment acknowledgment) {

        String originalTopic = record.topic().replace(".dlt", "");

        log.error("=== DLT HANDLER TRIGGERED ===");
        log.error("Original topic: {}", originalTopic);
        log.error("DLT topic: {}", record.topic());
        log.error("Partition: {}, Offset: {}", record.partition(), record.offset());
        log.error("Key: {}", record.key());

        if (record.value() instanceof String) {
            log.error("Value (String): {}", record.value());
            try {
                Object parsed = objectMapper.readValue((String) record.value(), Object.class);
                log.error("Parsed value: {}", parsed);
            } catch (JsonProcessingException e) {
                log.error("Value is not valid JSON: {}", record.value());
            }
        } else if (record.value() instanceof KafkaEventWrapper) {
            log.error("Value (KafkaEventWrapper): {}", record.value());
        } else {
            log.error("Value type: {}, Value: {}",
                    record.value() != null ? record.value().getClass().getName() : "null",
                    record.value());
        }
        record.headers().forEach(header -> {
            log.error("Header: {} = {}", header.key(),
                    header.value() != null ? new String(header.value()) : "null");
        });

        acknowledgment.acknowledge();
        log.error("=== DLT MESSAGE ACKNOWLEDGED ===");
    }
}