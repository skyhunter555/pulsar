package ru.syntez.integration.pulsar.sender;

import org.apache.pulsar.client.api.Producer;
import ru.syntez.integration.pulsar.entities.RoutingDocument;
import ru.syntez.integration.pulsar.usecases.SerializeDocumentUsecase;

import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PulsarSender {

    private final static Logger LOG = Logger.getLogger(PulsarSender.class.getName());

    private final Producer<byte[]> producer;

    public PulsarSender(Producer<byte[]> producer) {
        this.producer = producer;
    }

    public int send(
            RoutingDocumentGenerator docGenerator,
            MessageKeyGenerator keyGenerator,
            int times) {

        for (int index = 0; index < times; index++) {
            try {
                RoutingDocument document = docGenerator.create(index);
                Optional<String> msgKey = Optional.ofNullable(keyGenerator.generate(document));
                if (msgKey.isPresent())
                    producer.newMessage().key(msgKey.get()).value(SerializeDocumentUsecase.execute(document)).send();
                else
                    producer.newMessage().value(SerializeDocumentUsecase.execute(document)).send();

            } catch (Exception e) {
                LOG.log(Level.SEVERE, "Produce message exception was thrown", e);
                return index;
            }
        }
        return times;
    }

    public int sendWithDocIdKey(RoutingDocumentGenerator docGenerator, int times) {
        return send(docGenerator, doc -> String.format("key_%s", doc.getDocId()), times);
    }

    public int sendWithDocTypeKey(RoutingDocumentGenerator docGenerator, int times) {
        return send(docGenerator, doc -> String.format("%s_%s", doc.getDocType(), doc.getDocId()), times);
    }

}
