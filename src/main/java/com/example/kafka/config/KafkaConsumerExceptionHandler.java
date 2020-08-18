package com.example.kafka.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.RemainingRecordsErrorHandler;

import java.util.List;

@Slf4j
public class KafkaConsumerExceptionHandler implements RemainingRecordsErrorHandler {
    @Override
    public void handle(Exception thrownException, List<ConsumerRecord<?, ?>> records, Consumer<?, ?> consumer) {
        log.info("KafkaConsumerExceptionHandler => received: {}, remaining records: {}", thrownException.getMessage(), records.size());
    }
}
