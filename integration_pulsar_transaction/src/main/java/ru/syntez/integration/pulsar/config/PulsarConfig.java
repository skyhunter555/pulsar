package ru.syntez.integration.pulsar.config;

import lombok.Data;

@Data
public class PulsarConfig {

    private String brokersUrl;
    private String brokerPort;
    private Integer messageCount;
    private Integer timeoutBeforeConsume;
    private Integer maxRedeliveryCount;
    private Boolean recordLogOutputEnabled;
    private Integer operationTimeoutSeconds;
    private Integer connectTimeoutSeconds;

    private String  topic1Name;
    private String  topic2Name;
    private String  topicDeadLetterName;
}
