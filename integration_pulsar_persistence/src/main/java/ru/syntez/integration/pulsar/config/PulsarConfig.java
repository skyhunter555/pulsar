package ru.syntez.integration.pulsar.config;

import lombok.Data;

@Data
public class PulsarConfig {

    private String adminUrl;
    private String adminPort;
    private String brokersUrl;
    private String brokerPort;
    private String brokerPort1;
    private String brokerPort2;
    private String brokerPort3;
    private Integer messageCount;
    private Integer sendIntervalMs;
    private Integer receiveIntervalMs;
    private Integer timeoutBeforeConsume;
    private Integer maxRedeliveryCount;
    private Boolean recordLogOutputEnabled;
    private Integer operationTimeoutSeconds;
    private Integer connectTimeoutSeconds;

    private String  topicName;
    private String  topicNonPersistentName;
    private String  topicDeadLetterName;
}
