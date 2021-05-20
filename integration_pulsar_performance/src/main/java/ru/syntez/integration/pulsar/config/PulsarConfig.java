package ru.syntez.integration.pulsar.config;

import lombok.Data;
import ru.syntez.integration.pulsar.usecases.kafka.KafkaConfig;

@Data
public class PulsarConfig {

    private String brokersUrl;
    private String brokerPort;
    private Integer messageCount;
    private Integer timeoutBeforeConsume;
    private Integer timeoutReceive;
    private Integer maxRedeliveryCount;
    private Boolean recordLogOutputEnabled;
    private Integer operationTimeoutSeconds;
    private Integer connectTimeoutSeconds;

    private String topic1Name;
    private String topic10Name;
    private String topic100Name;
    private String topic1000Name;
    private String topicDeadLetterName;

    private KafkaConfig kafka;
}
