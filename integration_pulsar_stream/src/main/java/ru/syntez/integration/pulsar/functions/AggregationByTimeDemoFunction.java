package ru.syntez.integration.pulsar.functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pulsar.functions.api.*;
import ru.syntez.integration.pulsar.entities.RoutingDocument;

import java.util.Collection;
import java.util.Date;

public class AggregationByTimeDemoFunction implements WindowFunction<String, Void> {

    private final String TOPIC_OUTPUT = "topic-output-demo";
    private final ObjectMapper jsonMapper = new ObjectMapper();

    @Override
    public Void process(Collection<Record<String>> inputs, WindowContext context) throws Exception {

        Date nowDate = new Date();
        int documentsAmount = 0;

        context.getLogger().info(String.format("AggregationByTimeDemoFunction started=%s, recordsCount=%s", nowDate, inputs.size()));

        for (Record<String> record : inputs) {
            try {
                RoutingDocument document = jsonMapper.readValue(record.getValue(), RoutingDocument.class);
                documentsAmount = documentsAmount + document.getAmount();
            } catch (Exception ex) {
                context.getLogger().error(ex.getMessage());
            }
        }

        context.publish(
            TOPIC_OUTPUT,
            String.format("Result of aggregation amount: Date=%s, Count=%s, Amount=%s", nowDate, inputs.size(), documentsAmount)
        );

        return null;
    }
}


