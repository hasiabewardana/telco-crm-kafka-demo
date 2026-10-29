package lk.telco.crm.activation.api;

import lombok.Getter;

@Getter
public class EventPublishException extends RuntimeException {

    private final String eventId;

    public EventPublishException(String eventId, Throwable cause) {
        super("Could not publish event " + eventId + ": " + cause.getMessage(), cause);
        this.eventId = eventId;
    }
}
