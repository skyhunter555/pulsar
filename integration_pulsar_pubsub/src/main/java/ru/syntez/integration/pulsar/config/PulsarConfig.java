package ru.syntez.integration.pulsar.config;

import lombok.Data;

@Data
public class PulsarConfig {

    private String  brokers;
    private Integer messageCount;
    private Integer timeoutBeforeConsume;
    private ProducerConfig producer;

    private String  topicAtleastName;
    private String  topicAtmostName;
    private String  topicEffectivelyName;
    private String  topicEffectivelyCompactedName;
    private String  topicName;
}
