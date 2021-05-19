package ru.syntez.integration.pulsar.sender;

import org.apache.pulsar.client.api.Producer;
import ru.syntez.integration.pulsar.entities.DataSizeEnum;
import ru.syntez.integration.pulsar.entities.RoutingDocument;
import ru.syntez.integration.pulsar.usecases.SerializeDocumentUsecase;

import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PulsarSender {

    private final static Logger LOG = Logger.getLogger(PulsarSender.class.getName());
    private final String payload1Kb = "This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload.";
    private final Producer<byte[]> producer;

    public PulsarSender(Producer<byte[]> producer) {
        this.producer = producer;
    }

    public int send(
            RoutingDocumentGenerator docGenerator,
            MessageKeyGenerator keyGenerator,
            int times,
            DataSizeEnum size,
            Boolean recordLogOutputEnabled
    ) {

        StringBuffer docData = new StringBuffer();
        for (int index = 0; index < size.getFactor(); index++) {
            docData.append(payload1Kb);
        }

        for (int index = 0; index < times; index++) {
            try {
                RoutingDocument document = docGenerator.create(index, docData.toString());
                Optional<String> msgKey = Optional.ofNullable(keyGenerator.generate(document));
                producer.newMessage()
                        .key(msgKey.get())
                        .value(SerializeDocumentUsecase.execute(document))
                        .send();

                if (recordLogOutputEnabled) {
                    LOG.info(String.format("Produce message %s with key=%s to topic", index, msgKey.get()));
                }
            } catch (Exception e) {
                LOG.log(Level.WARNING, "Produce message exception was thrown", e);
                return index;
            }
        }
        return times;
    }


    public int sendWithDocData(RoutingDocumentGenerator docGenerator, int times, DataSizeEnum size, Boolean recordLogOutputEnabled) {
        return send(docGenerator, doc -> String.format("%s_%s", doc.getDocType(), doc.getDocId()), times, size, recordLogOutputEnabled);
    }

}
