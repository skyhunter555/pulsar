package ru.syntez.integration.pulsar.scenarios;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.transaction.Transaction;
import ru.syntez.integration.pulsar.config.PulsarConfig;
import ru.syntez.integration.pulsar.entities.RoutingDocument;
import ru.syntez.integration.pulsar.sender.PulsarSender;
import ru.syntez.integration.pulsar.usecases.create.ProducerCreatorUsecase;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Реализация тестового сценария при котором будут записаны сообщения с уникальным ключем в рамках транзакции
 */
public class ProducerWithTransaction implements ProducerTestScenario {
    private final static Logger LOG = Logger.getLogger(ProducerWithTransaction.class.getName());

    private final PulsarClient client;
    private final PulsarConfig config;

    public ProducerWithTransaction(PulsarClient client, PulsarConfig config) {
        this.client = client;
        this.config = config;
    }

    @Override
    public int run(String topicName, Transaction txn) {
        try (Producer<byte[]> producer = ProducerCreatorUsecase.execute(client, topicName)) {
            PulsarSender sender = new PulsarSender(producer);
            int sentCount = sender.sendWithDocTypeKey(
                    RoutingDocument::createAny,
                    config.getMessageCount(),0, txn
            );
            producer.flush();
            return sentCount;
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Error producing documents to pulsar", e);
            return 0;
        }
    }

}
