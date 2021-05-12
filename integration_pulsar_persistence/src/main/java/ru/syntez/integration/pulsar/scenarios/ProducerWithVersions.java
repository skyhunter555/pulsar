package ru.syntez.integration.pulsar.scenarios;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import ru.syntez.integration.pulsar.config.PulsarConfig;
import ru.syntez.integration.pulsar.entities.RoutingDocument;
import ru.syntez.integration.pulsar.sender.PulsarSender;
import ru.syntez.integration.pulsar.usecases.create.ProducerCreatorUsecase;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Реализация тестового сценария при котором буду записаны три версии сообщения на каждый уникальный ключ
 * Для кейсов по проверке EFFECTIVELY_ONCE
 */
public class ProducerWithVersions implements ProducerTestScenario {

    private final static Logger LOG = Logger.getLogger(ProducerWithVersions.class.getName());

    private final PulsarClient client;
    private final PulsarConfig config;

    public ProducerWithVersions(PulsarClient client, PulsarConfig config) {
        this.client = client;
        this.config = config;
    }

    @Override
    public int run(String topicName) {

        try (Producer<byte[]> producer = ProducerCreatorUsecase.execute(client, topicName)) {

            PulsarSender sender = new PulsarSender(producer);

            int unknownDocsCount = sender.sendWithDocIdKey(
                    RoutingDocument::createUnknown,
                    config.getMessageCount()
            );

            int orderDocsCount = sender.sendWithDocIdKey(
                    RoutingDocument::createOrder,
                    config.getMessageCount()
            );

            int invoiceDocsCount = sender.sendWithDocIdKey(
                    RoutingDocument::createInvoice,
                    config.getMessageCount()
            );

            producer.flush();
            return unknownDocsCount + invoiceDocsCount + orderDocsCount;

        } catch (PulsarClientException e) {
            LOG.log(Level.SEVERE, "Error producing documents to pulsar", e);
            return 0;
        }
    }
}
