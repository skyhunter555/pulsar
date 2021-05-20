package ru.syntez.integration.pulsar.scenarios;

import org.apache.pulsar.client.api.transaction.Transaction;
import ru.syntez.integration.pulsar.entities.DataSizeEnum;
import ru.syntez.integration.pulsar.entities.ResultReport;

public interface ProducerTestScenario {

    ResultReport run(String topicName, DataSizeEnum size, String producerId);

    ResultReport run(String topicName, Transaction txn, String producerId);
}
