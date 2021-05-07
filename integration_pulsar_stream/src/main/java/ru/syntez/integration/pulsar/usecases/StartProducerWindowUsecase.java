package ru.syntez.integration.pulsar.usecases;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import ru.syntez.integration.pulsar.entities.DocumentTypeEnum;
import ru.syntez.integration.pulsar.entities.OutputDocumentExt;
import ru.syntez.integration.pulsar.entities.RoutingDocument;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class StartProducerWindowUsecase {

    private final static Logger LOG = Logger.getLogger(StartProducerWindowUsecase.class.getName());

    public static Integer execute(PulsarClient client, String topicName, Integer messageCount) throws PulsarClientException, JsonProcessingException {

        AtomicInteger msgSentCounter = new AtomicInteger(0);

        Producer<byte[]> producer = client.newProducer()
                .topic(topicName)
                .compressionType(CompressionType.LZ4)
                .create();

        RoutingDocument document = new RoutingDocument();
        for (int index = 0; index < messageCount; index++) {
            document.setDocId(index);
            document.setAmount(10);
            if ((index % 2) == 0) {
                document.setDocType(DocumentTypeEnum.order);
            } else {
                document.setDocType(DocumentTypeEnum.invoice);
            }
            byte[] msgValue = SerializeJsonDocumentUsecase.execute(document);
            String messageKey = document.getDocType().name() + "_" + getMessageKey();
            producer.newMessage()
                    .key(messageKey)
                    .value(msgValue)
                    .send();
            msgSentCounter.incrementAndGet();
            LOG.info(String.format("Send messageIndex=%s; key=%s; topic=%s; message=%s", index, messageKey, topicName, msgValue));
        }
        producer.flush();
        LOG.info(String.format("Количество отправленных уникальных сообщений: %s", msgSentCounter.get()));
        return msgSentCounter.get();
    }

    private static String getMessageKey() {
        return UUID.randomUUID().toString();
    }

}
