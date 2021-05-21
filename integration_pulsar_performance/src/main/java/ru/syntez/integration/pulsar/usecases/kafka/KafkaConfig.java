package ru.syntez.integration.pulsar.usecases.kafka;

import lombok.Data;

@Data
public class KafkaConfig {

    private String  brokers;
    private Integer producerCount;
    private Integer messageCount;
    private String  topicName;
    private String  groupIdConfig;
    private ru.syntez.integration.pulsar.usecases.kafka.ConsumerConfig consumer;
    private ProducerConfig producer;
}
