package ru.syntez.integration.pulsar.usecases.kafka;

import lombok.Data;

@Data
public class ConsumerConfig {

    private String  offsetResetLatest;
    private String  offsetResetEarlier;
    private Integer maxPoolRecords;
    private String  clientId;
    private String  clientRack;
    private String  partitionAssignmentStrategy;

}
