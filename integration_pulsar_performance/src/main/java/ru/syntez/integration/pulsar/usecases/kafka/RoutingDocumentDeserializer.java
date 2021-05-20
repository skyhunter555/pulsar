package ru.syntez.integration.pulsar.usecases.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import ru.syntez.integration.pulsar.entities.RoutingDocument;

import java.util.Map;

public class RoutingDocumentDeserializer implements Deserializer<RoutingDocument> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public RoutingDocument deserialize(String topic, byte[] data) {
        ObjectMapper mapper = new ObjectMapper();
        RoutingDocument object = null;
        try {
            object = mapper.readValue(data, RoutingDocument.class);
        } catch (Exception exception) {
            System.out.println("Error in deserializing bytes "+ exception);
        }
        return object;
    }

    @Override
    public void close() {
    }
}
