package com.example.kafka.service;

import com.example.kafka.DummyModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

@Service
public class ProducerService {
    private final static Logger log = LoggerFactory.getLogger(ProducerService.class);
    private KafkaTemplate<String, DummyModel> producer;
    private int counter = 0;
    private int batchSize = 1_00_000;

    public ProducerService(KafkaTemplate<String, DummyModel> producer) {
        this.producer = producer;
    }

    public void send() {
        for (int ii = 0; ii < batchSize; ii++) {
            counter++;
            String key = "" + UUID.randomUUID().toString();
            DummyModel model = DummyModel.instance();
            try {
                producer.send("test-topic", key, model).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }
}
