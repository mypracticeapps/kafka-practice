package com.example.kafka.config;

import com.example.kafka.DummyModel;
import com.example.kafka.util.ThreadUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.MessageListener;

@Slf4j
public class KafkaConsumerListener implements MessageListener<String, DummyModel> {
    private int count = 0;

    @Override
    public void onMessage(ConsumerRecord<String, DummyModel> data) {
        ThreadUtil.sleep(3000);
        log.info("[ABOUT TO SUCCESS] Topic: {} Partition: {} Key: {} Value: {}", data.topic(), data.partition(), data.key(), data.value());

//        count++;
//        if (count == 3 || count == 8) {
//            log.info("[ABOUT TO FAIL] Topic: {} Partition: {} Key: {} Value: {}", data.topic(), data.partition(), data.key(), data.value());
//            throw new RuntimeException("manual fail");
//        } else {
//            log.info("[ABOUT TO SUCCESS] Topic: {} Partition: {} Key: {} Value: {}", data.topic(), data.partition(), data.key(), data.value());
//        }
    }
}
