package ru.syntez.integration.pulsar.pulsar;

import lombok.Data;

@Data
public class PulsarConfig {

    private String  brokers;
    private Integer messageCount;
    private String  topicInputRouteName;
    private String  topicInputGroupName;
    private String  topicOutputOrderName;
    private String  topicOutputInvoiceName;
    private String  topicOutputGroupName;

}
