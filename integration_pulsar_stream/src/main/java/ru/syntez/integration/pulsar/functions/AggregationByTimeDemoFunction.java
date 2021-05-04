package ru.syntez.integration.pulsar.functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pulsar.functions.api.*;
import ru.syntez.integration.pulsar.entities.RoutingDocument;
import java.util.Collection;
import java.util.Date;

public class AggregationByTimeDemoFunction implements WindowFunction<String, String> {

    private ObjectMapper jsonMapper = new ObjectMapper();
    private long lastDurationMs = 60000;

    @Override
    public String process(Collection<Record<String>> inputs, WindowContext context) {

        int documentsAmount = 0;
        int recordsCount = 0;
        Date nowDate = new Date();
        context.getLogger().info(String.format("AggregationByTimeDemoFunction started=%s, recordsCount=%s", nowDate, inputs.size()));
        for (Record<String> record : inputs) {

            context.getLogger().info(String.format("AggregationByTimeDemoFunction record value=%s", record));

            if (record.getEventTime().isPresent()) {
                Long eventTimestamp =  record.getEventTime().get();
                if (nowDate.getTime() - eventTimestamp < lastDurationMs) {
                    RoutingDocument document;
                    try {
                        document = jsonMapper.readValue(record.getValue(), RoutingDocument.class);
                    } catch (Exception ex) {
                        context.getLogger().error("Failed to read JSON string: " + ex.getMessage());
                        return null;
                    }
                    context.getLogger().info(String.format("recordEventTime=%s, documentsAmount=%s", new Date(eventTimestamp), documentsAmount));
                    documentsAmount = documentsAmount + document.getAmount();
                    recordsCount++;
                }
            }

        }
        return String.format("Calculated documentsAmount=%s from recordCount=%s", documentsAmount, recordsCount);
    }
}


