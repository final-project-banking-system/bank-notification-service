package banking.notification.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaRetryTopic;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
@EnableKafkaRetryTopic
public class KafkaConfig {
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public TaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.initialize();
        return scheduler;
    }

    @Bean
    public ConsumerFactory<String, Object> consumerManualCommitFactory() {
        final Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);

        config.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);

        config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                KafkaProperties.IsolationLevel.READ_COMMITTED.toString().toLowerCase());

        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "5000");
        config.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, "5000");
        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerManualCommitContainerFactory(
            ConsumerFactory<String, Object> consumerManualCommitFactory
    ) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerManualCommitFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                new FixedBackOff(0L, 0)
        );
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }
}
