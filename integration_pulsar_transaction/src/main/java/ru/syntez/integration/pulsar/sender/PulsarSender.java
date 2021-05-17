package ru.syntez.integration.pulsar.sender;

import org.apache.pulsar.client.api.Producer;
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
            List<String> replicationClusters
    ) {

        for (int index = 0; index < times; index++) {
            try {
                RoutingDocument document = docGenerator.create(index);
                Optional<String> msgKey = Optional.ofNullable(keyGenerator.generate(document));

                if (msgKey.isPresent())
                    producer.newMessage()
                            .key(msgKey.get())
                            .value(SerializeDocumentUsecase.execute(document))
                            .replicationClusters(replicationClusters)
                            .send();
                else
                    producer.newMessage().value(SerializeDocumentUsecase.execute(document)).send();

                System.out.println(String.format("Produce message %s with key=%s to topic", index, msgKey.get()));
                Thread.sleep(intervalMs);

            } catch (Exception e) {
                LOG.log(Level.WARNING, "Produce message exception was thrown", e);
                return index;
            }
        }
        return times;
    }

    public int sendWithDocIdKey(RoutingDocumentGenerator docGenerator, int times, int intervalMs, List<String> replicationClusters) {
        return send(docGenerator, doc -> String.format("key_%s", doc.getDocId()), times, intervalMs, replicationClusters);
    }

    public int sendWithDocTypeKey(RoutingDocumentGenerator docGenerator, int times, int intervalMs) {
        List<String> replicationClusters = Arrays.asList("standalone");
        return send(docGenerator, doc -> String.format("%s_%s", doc.getDocType(), doc.getDocId()), times, intervalMs, replicationClusters);
    }

    public int sendToCluster(RoutingDocumentGenerator docGenerator, int times, int intervalMs, List<String> replicationClusters) {
        return send(docGenerator, doc -> String.format("key_%s", doc.getDocId()), times, intervalMs, replicationClusters);
    }

}
