package lk.telco.crm.activation.publisher;

import lk.telco.crm.events.ServiceActivationEvent;
import lk.telco.crm.events.Topics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

import java.nio.charset.StandardCharsets;

/**
 * Publishes service activation events. Knows nothing about who consumes them.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ActivationEventPublisher {

    private final KafkaTemplate<String, ServiceActivationEvent> kafkaTemplate;

    @Value("${spring.application.name}")
    private String source;

    public ListenableFuture<SendResult<String, ServiceActivationEvent>> publish(ServiceActivationEvent event) {
        // key = msisdn -> same subscriber always goes to the same partition -> per-subscriber ordering
        ProducerRecord<String, ServiceActivationEvent> record =
                new ProducerRecord<>(Topics.SERVICE_ACTIVATION, event.getMsisdn(), event);

        // Headers = metadata about the event, readable without deserializing the payload
        record.headers().add("eventType", ("SERVICE_" + event.getAction().name()).getBytes(StandardCharsets.UTF_8));
        record.headers().add("source", source.getBytes(StandardCharsets.UTF_8));

        ListenableFuture<SendResult<String, ServiceActivationEvent>> future = kafkaTemplate.send(record);

        // send() is asynchronous: the callback runs on the producer's I/O thread when the broker answers
        future.addCallback(
                result -> log.info("Published {} msisdn={} -> partition={} offset={}",
                        event.getEventId(), event.getMsisdn(),
                        result.getRecordMetadata().partition(), result.getRecordMetadata().offset()),
                ex -> log.error("Failed to publish {} msisdn={}: {}",
                        event.getEventId(), event.getMsisdn(), ex.getMessage()));

        return future;
    }
}
