package com.example.kafka.config;

import com.example.kafka.DummyModel;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.*;
import org.springframework.util.backoff.FixedBackOff;

import java.util.*;
import java.util.stream.Collectors;

@Configuration
//@EnableKafka
@Slf4j
public class KafkaConsumerConfig {
    private Map<String, Object> consumerConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.23:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group3");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "3000000");

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, "1000");
        return props;
    }

    @Bean
    public ConsumerFactory<String, DummyModel> consumerFactory() {
        DefaultKafkaConsumerFactory<String, DummyModel> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerConfig());
        return consumerFactory;
    }

    private ContainerProperties containerProperties() {
        ContainerProperties containerProperties = new ContainerProperties("test-topic");
        containerProperties.setConsumerRebalanceListener(new KafkaConsumerRebalanceListener());
        containerProperties.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

//        containerProperties.setMessageListener(new KafkaConsumerListener());
        containerProperties.setMessageListener(new KafkaConsumerBatchListener());

        return containerProperties;
    }

    @Bean
    public MessageListenerContainer listenerContainer() {
        ConsumerFactory<String, DummyModel> consumerFactory = consumerFactory();
        ContainerProperties containerProperties = containerProperties();
        ConcurrentMessageListenerContainer listenerContainer = new ConcurrentMessageListenerContainer(consumerFactory, containerProperties);
        listenerContainer.setConcurrency(10);
//        listenerContainer.setErrorHandler(new KafkaConsumerExceptionHandler());
        SeekToCurrentErrorHandler errorHandler = new SeekToCurrentErrorHandler(new FixedBackOff(1000L, 3L));
//        errorHandler.setAckAfterHandle(false);
//        listenerContainer.setErrorHandler(errorHandler);
        return listenerContainer;
    }
}
