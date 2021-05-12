package ru.syntez.integration.pulsar.config;

import lombok.Data;

@Data
public class PulsarConfig {

    private String  brokers;
    private Integer messageCount;
    private Integer timeoutBeforeConsume;
    private Integer maxRedeliveryCount;
    private Boolean recordLogOutputEnabled;
    private Integer operationTimeoutSeconds;
    private Integer connectTimeoutSeconds;
    private Integer errorMessageCount;

    private String  topicAtleastName;
    private String  topicAtmostName;
    private String  topicEffectivelyName;
    private String  topicEffectivelyCompactedName;
    private String  topicName;
    private String  topicInputRouteName;
    private String  topicInputFilterName;
    private String  topicOutputOrderName;
    private String  topicOutputInvoiceName;
    private String  topicOutputFilterName;
    private String  topicDeadLetterName;
}
