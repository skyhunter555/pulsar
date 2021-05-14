package ru.syntez.integration.pulsar.scenarios;

import java.util.List;

public interface ProducerTestScenario {
    int run(String topicName);

    int run(String topicName, List<String> replicationClusters);

}
