package banking.notification.config;

import lombok.Data;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Data
@Configuration
@ConfigurationProperties(prefix = "spring.kafka")
public class KafkaTopicConfig {
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${banking.kafka.topics.logins}")
    private String loginsTopic;

    @Value("${banking.kafka.topics.transfers}")
    private String transfersTopic;

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        return new KafkaAdmin(configs);
    }

    @Bean
    public NewTopic loginsTopic() {
        return new NewTopic(loginsTopic, 1, (short) 1);
    }

    @Bean
    public NewTopic loginsDqTopic() {
        return new NewTopic(loginsTopic + ".dq", 1, (short) 1);
    }

    @Bean
    public NewTopic transfersTopic() {
        return new NewTopic(transfersTopic, 1, (short) 1);
    }

    @Bean
    public NewTopic transfersDqTopic() {
        return new NewTopic(transfersTopic + ".dq", 1, (short) 1);
    }
}
