package lk.telco.crm.events;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Published to {@link Topics#SERVICE_ACTIVATION} whenever a subscriber's value-added service
 * is activated or deactivated. Record key = msisdn, so all events of one subscriber stay in order.
 *
 * <p>The event carries everything consumers need (fee, channel, ...) so they never have to call
 * back into the CRM ("event-carried state transfer").
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ServiceActivationEvent {

    /** Unique id per event; consumers use it to detect duplicates (Phase 5). */
    private String eventId;

    /** When the business fact happened (not when Kafka received it). ISO-8601 on the wire, e.g. 2023-05-01T10:15:30Z */
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant occurredAt;

    private String msisdn;

    /** e.g. 4G_DATA_10GB, IDD, ROAMING */
    private String serviceCode;

    private ActivationAction action;

    /** Monthly fee in LKR; billing charges this on activation. */
    private BigDecimal fee;

    /** Where the request came from: APP, USSD, RETAIL, CALL_CENTER */
    private String channel;
}
