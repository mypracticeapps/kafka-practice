package com.example.kafka.config;

import com.example.kafka.DummyModel;
import com.example.kafka.util.ThreadUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.Acknowledgment;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Slf4j
public class KafkaConsumerListener implements MessageListener<String, DummyModel> {
    private int count = 0;

    @Override
    public void onMessage(ConsumerRecord<String, DummyModel> data) {
        ThreadUtil.sleep(3000);
        log.info("Topic: {} Partition: {} Key: {} Value: {}", data.topic(), data.partition(), data.key(), data.value());


        ThreadUtil.sleep(3000);
    }
}
