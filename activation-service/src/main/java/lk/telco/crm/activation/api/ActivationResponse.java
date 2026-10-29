package lk.telco.crm.activation.api;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Returned once Kafka has durably stored the event. Topic/partition/offset are exposed only to
 * make this learning project observable; a real API would return just the eventId.
 */
@Data
@AllArgsConstructor
public class ActivationResponse {

    private String eventId;
    private String status;
    private String topic;
    private int partition;
    private long offset;
}
