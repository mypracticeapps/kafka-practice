package com.example.kafka.config;

import com.example.kafka.DummyModel;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    private Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.23:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "1000");
        return props;
    }

    @Bean
    public ProducerFactory<String, DummyModel> producerFactory() {
        DefaultKafkaProducerFactory<String, DummyModel> producerFactory = new DefaultKafkaProducerFactory<>(producerConfigs());
        return producerFactory;
    }

    @Bean
    public KafkaTemplate<String, DummyModel> kafkaTemplate() {
        KafkaTemplate<String, DummyModel> template = new KafkaTemplate<>(producerFactory());
        return template;
    }

}