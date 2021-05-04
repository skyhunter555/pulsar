package ru.syntez.integration.pulsar.pulsar;

import lombok.Data;

@Data
public class PulsarConfig {

    private String  brokers;
    private Integer messageCount;
    private String  topicInputName;
    private String  topicInputOrderName;
    private String  topicInputInvoiceName;
    private String  topicOutputOrderName;
    private String  topicOutputInvoiceName;
    private String  topicOutputGroupName;
    private String  topicInputJsonName;
}
