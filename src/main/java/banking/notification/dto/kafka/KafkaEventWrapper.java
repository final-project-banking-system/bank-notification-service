package banking.notification.dto.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.Data;

@Data
public class KafkaEventWrapper {
    @JsonProperty("data")
    public JsonNode data;

    @JsonProperty("eventType")
    public String eventType;
}
