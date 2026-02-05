package banking.notification.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "unisender")
public class UnisenderConfig {
    private String apiKey;
    private String apiUrl = "https://api.unisender.com";
    private String senderName;
    private String senderEmail;
    private String defaultListId = "1";

    private int maxRetries = 3;
    private long initialDelay = 1000;
    private double multiplier = 2.0;
    private long maxDelay = 10000;

}
