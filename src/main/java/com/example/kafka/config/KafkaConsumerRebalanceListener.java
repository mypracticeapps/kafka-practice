package com.example.kafka.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class KafkaConsumerRebalanceListener implements ConsumerAwareRebalanceListener {
    private static Set<String> partitionSet = new HashSet<>();

    @Override
    public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        List<String> partitionsList = partitions.stream().map(p -> p.partition() + "").collect(Collectors.toList());
        partitionSet.removeAll(partitionsList);
        String assignedPartitions = String.join(", ", partitionSet);
        log.info("KafkaConsumerConfig.onPartitionsRevokedBeforeCommit() >> {}", assignedPartitions);
    }

    @Override
    public void onPartitionsRevokedAfterCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        log.info("KafkaConsumerConfig.onPartitionsRevokedAfterCommit()");
    }

    @Override
    public void onPartitionsLost(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        log.info("KafkaConsumerConfig.onPartitionsLost()");
    }

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        List<String> partitionsList = partitions.stream().map(p -> p.partition() + "").collect(Collectors.toList());
        partitionSet.addAll(partitionsList);
        String assignedPartitions = String.join(", ", partitionSet);
        log.info("KafkaConsumerConfig.onPartitionsAssigned() >> {}", assignedPartitions);
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        List<String> partitionsList = partitions.stream().map(p -> p.partition() + "").collect(Collectors.toList());
        partitionSet.removeAll(partitionsList);
        String assignedPartitions = String.join(", ", partitionSet);
        log.info("KafkaConsumerConfig.onPartitionsRevoked() >> {}", assignedPartitions);

    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.info("KafkaConsumerConfig.onPartitionsAssigned()");
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        log.info("KafkaConsumerConfig.onPartitionsLost()");
    }
}
