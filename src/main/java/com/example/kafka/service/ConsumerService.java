package com.example.kafka.service;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

//@Service
public class ConsumerService {
    private final static Logger log = LoggerFactory.getLogger(ConsumerService.class);
    private KafkaConsumer<String, String> consumer;

    public ConsumerService() {
        Properties properties = defaultProperties();
        this.reConfigureConsumer(properties);
    }

    public void pollOnce() {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
        for (ConsumerRecord record : records) {
            log.info("Record: key: {} Partition: {}, Offset: {}, Value: [{}]", record.key(), record.value(), record.partition(), record.offset());
        }
    }

    private Properties defaultProperties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.123:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-topic-test-consumer-group");

        properties.setProperty(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "true");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }

    public void reConfigureConsumer(Properties properties) {
        if (consumer != null) {
            consumer.close(Duration.ofMillis(100));
        }

        this.consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList("test-topic"));
    }
}
