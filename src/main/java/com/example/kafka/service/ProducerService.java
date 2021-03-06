package com.example.kafka.service;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Properties;

@Service
public class ProducerService {
    private final static Logger log = LoggerFactory.getLogger(ProducerService.class);
    private KafkaProducer<String, String> producer;
    private int counter = 0;
    private int batchSize = 10;

    public ProducerService() {
        Properties properties = this.defaultProperties();
        this.reConfigureProducer(properties);
    }

    public void send() {
        for (int ii = 0; ii < batchSize; ii++) {
            counter++;
            ProducerRecord record = new ProducerRecord("test-topic", Integer.toString(counter), "Message: " + counter);
            producer.send(record, ((metadata, exception) -> {
                if (exception != null) {
                    log.warn("Exception while sending message: {}", exception.getMessage());
                } else {
                    log.info("Message sent to partition: {} with offset: {}", metadata.partition(), metadata.offset());
                }
            }));
        }
//        producer.flush();
    }

    private Properties defaultProperties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.123:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }

    public void reConfigureProducer(Properties properties) {
        if (producer != null) {
            producer.flush();
            producer.close(Duration.ofMillis(100));
        }

        this.producer = new KafkaProducer<String, String>(properties);
    }
}
