package com.example.kafka.config;

import com.example.kafka.DummyModel;
import com.example.kafka.util.ThreadUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.BatchConsumerAwareMessageListener;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Slf4j
public class KafkaConsumerBatchListener implements BatchConsumerAwareMessageListener<String, DummyModel> {

    public void onMessage(List<ConsumerRecord<String, DummyModel>> data, Consumer<?, ?> consumer) {
        Map<Integer, List<ConsumerRecord<String, DummyModel>>> partitionGroup = groupByPartition(data);
        log.info("BATCH SIZE: {} TOTAL PARTITIONS: {}", data.size(), partitionGroup.size());

        for (List<ConsumerRecord<String, DummyModel>> records : partitionGroup.values()) {
            List<List<ConsumerRecord<String, DummyModel>>> chunks = batches(records, 50).collect(Collectors.toList());

            chunks.forEach(l -> {
                this.proccesInAsync(l, consumer);
            });
        }
    }

    public void proccesInAsync(List<ConsumerRecord<String, DummyModel>> l, Consumer consumer) {
        ConsumerRecord last = l.get(l.size() - 1);
        log.info("\t processing partition: {} chunk size: {} first offset: {} last offset: {}", last.partition(), l.size(), l.get(0).offset(), last.offset());
        this.commit(consumer, last.partition(), last.offset());
    }

    public static Map<Integer, List<ConsumerRecord<String, DummyModel>>> groupByPartition(List<ConsumerRecord<String, DummyModel>> data) {
        Map<Integer, List<ConsumerRecord<String, DummyModel>>> partitionGroup = new HashMap<>();

        for (ConsumerRecord record : data) {
            List<ConsumerRecord<String, DummyModel>> records = partitionGroup.get(record.partition());
            if (records == null) {
                records = new LinkedList<>();
                partitionGroup.put(record.partition(), records);
            }
            records.add(record);
        }

        return partitionGroup;
    }

    public static <T> Stream<List<T>> batches(List<T> source, int length) {
        if (length <= 0)
            throw new IllegalArgumentException("length = " + length);
        int size = source.size();
        if (size <= 0)
            return Stream.empty();
        int fullChunks = (size - 1) / length;
        return IntStream.range(0, fullChunks + 1).mapToObj(
                n -> source.subList(n * length, n == fullChunks ? size : (n + 1) * length));
    }

    private void commit(Consumer consumer, int partition, long offset) {
        Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();

        TopicPartition topicPartition = new TopicPartition("test-topic", partition);
        OffsetAndMetadata metadata = new OffsetAndMetadata(offset);

        offsetMap.put(topicPartition, metadata);
        consumer.commitSync(offsetMap);

        ThreadUtil.sleep(3000);
    }

    private void printCount(List<ConsumerRecord<String, DummyModel>> data) {
        Map<Integer, Integer> counter = new HashMap<>();
        for (ConsumerRecord record : data) {
            int partition = record.partition();
            if (counter.containsKey(partition)) {
                int num = counter.get(partition);
                num++;
                counter.put(partition, num);
            } else {
                counter.put(partition, 1);
            }
        }

        List<List<ConsumerRecord<String, DummyModel>>> chunks = batches(data, 100).collect(Collectors.toList());
        log.info("BATCH INFO: {}", counter);

    }
}
