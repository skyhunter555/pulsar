package ru.syntez.integration.pulsar.usecases;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import ru.syntez.integration.pulsar.entities.DocumentTypeEnum;
import ru.syntez.integration.pulsar.entities.OutputDocumentExt;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class StartProducerUsecase {

    private final static Logger LOG = Logger.getLogger(StartProducerUsecase.class.getName());

    public static Integer execute(PulsarClient client, String topicName, Integer messageCount) throws PulsarClientException, JsonProcessingException {

        AtomicInteger msgSentCounter = new AtomicInteger(0);

        OutputDocumentExt document = new OutputDocumentExt();
        Producer<byte[]> producer = client.newProducer()
                .topic(topicName)
                .compressionType(CompressionType.LZ4)
                .create();
        for (int index = 0; index < messageCount; index++) {
            document.setDocumentId(index);
            document.setDocumentNumber(index + 100);
            if ((index % 2) == 0) {
                document.setDocumentType(DocumentTypeEnum.order);
            } else {
                document.setDocumentType(DocumentTypeEnum.invoice);
            }
            byte[] msgValue = SerializeDocumentUsecase.execute(document);
            String messageKey = document.getDocumentType().name() + "_" + getMessageKey();
            producer.newMessage()
                    .key(messageKey)
                    .value(msgValue)
                    .send();
            msgSentCounter.incrementAndGet();
            LOG.info("Send message " + index + "; Key=" + messageKey + "; topic = " + topicName);
        }
        producer.flush();
        LOG.info(String.format("Количество отправленных уникальных сообщений: %s", msgSentCounter.get()));
        return msgSentCounter.get();
    }

    private static String getMessageKey() {
        return UUID.randomUUID().toString();
    }

}
