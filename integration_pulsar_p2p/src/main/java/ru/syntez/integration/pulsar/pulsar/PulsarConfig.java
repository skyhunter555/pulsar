package ru.syntez.integration.pulsar.pulsar;

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
    private String  topicName;
    private String  topicInputRouteName;
    private String  topicInputFilterName;
    private String  topicOutputOrderName;
    private String  topicOutputInvoiceName;
    private String  topicOutputFilterName;

}
