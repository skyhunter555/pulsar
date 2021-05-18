package ru.syntez.integration.pulsar.scenarios;

import org.apache.pulsar.client.api.transaction.Transaction;

public interface ProducerTestScenario {
    int run(String topicName, Transaction txn);
}
