package lk.telco.crm.activation.config;

import lk.telco.crm.events.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * The producer owns its topic. On startup Spring's KafkaAdmin creates any NewTopic bean that does
 * not exist yet; existing topics are left untouched (it never deletes or shrinks partitions).
 */
@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic serviceActivationTopic() {
        return TopicBuilder.name(Topics.SERVICE_ACTIVATION)
                .partitions(3)
                .replicas(3)
                .config("min.insync.replicas", "2")
                .build();
    }
}
