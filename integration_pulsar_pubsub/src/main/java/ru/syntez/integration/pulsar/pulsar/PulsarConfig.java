package ru.syntez.integration.pulsar.pulsar;

import lombok.Data;

@Data
public class PulsarConfig {

    private String  brokers;
    private Integer messageCount;
    private String  topicName;
    private Integer timeoutBeforeConsume;
    private ProducerConfig producer;
}
