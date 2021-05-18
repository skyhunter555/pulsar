package ru.syntez.integration.pulsar.sender;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.transaction.Transaction;
import ru.syntez.integration.pulsar.entities.RoutingDocument;
import ru.syntez.integration.pulsar.usecases.SerializeDocumentUsecase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
            int times,
            int intervalMs,
            Transaction txn
    ) {

        for (int index = 0; index < times; index++) {
            try {
                RoutingDocument document = docGenerator.create(index);
                Optional<String> msgKey = Optional.ofNullable(keyGenerator.generate(document));

                producer.newMessage(txn)
                        .key(msgKey.get())
                        .value(SerializeDocumentUsecase.execute(document))
                        .sendAsync();

                LOG.info(String.format("Produce message %s with key=%s to topic", index, msgKey.get()));
                Thread.sleep(intervalMs);
            } catch (Exception e) {
                LOG.log(Level.WARNING, "Produce message exception was thrown", e);
                return index;
            }
        }
        return times;
    }


    public int sendWithDocTypeKey(RoutingDocumentGenerator docGenerator, int times, int intervalMs, Transaction txn) {
        return send(docGenerator, doc -> String.format("%s_%s", doc.getDocType(), doc.getDocId()), times, intervalMs, txn);
    }

}
