package banking.notification.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;

@Configuration
public class RestClientConfig {
    @Bean
    public RestClient unsisenderRestClient() {
        return RestClient.builder()
                .baseUrl("https://api.unisender.com")
                .defaultHeader("Content-Type", "application/json")
                .requestInterceptor(
                        (request, body, execution) -> execution.execute(request, body))
                .build();
    }
}
