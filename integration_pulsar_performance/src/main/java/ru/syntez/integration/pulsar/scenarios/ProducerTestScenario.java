package ru.syntez.integration.pulsar.scenarios;

import org.apache.pulsar.client.api.transaction.Transaction;
import ru.syntez.integration.pulsar.entities.DataSizeEnum;

public interface ProducerTestScenario {

    int run(String topicName, DataSizeEnum size, String producerId);

    int run(String topicName, Transaction txn, String producerId);
}
