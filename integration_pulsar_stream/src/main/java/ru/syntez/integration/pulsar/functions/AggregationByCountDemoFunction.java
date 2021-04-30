package ru.syntez.integration.pulsar.functions;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

import java.util.HashMap;
import java.util.Map;

public class AggregationByCountDemoFunction implements Function<String, Void> {

    private final String TOPIC_OUTPUT_ORDER = "topic-output-order-demo";
    private final String TOPIC_OUTPUT_INVOICE = "topic-output-invoice-demo";

    @Override
    public Void process(String input, Context context) {
        //context.getLogger().info("RoutingByKeyDemoFunction check!!! context:" + context.toString());
        Map<String, String> properties = new HashMap<>();
        properties.put("input_topic", context.getCurrentRecord().getTopicName().get());
        properties.putAll(context.getCurrentRecord().getProperties());
        try {
            String outputTopicName = TOPIC_OUTPUT_ORDER;
            if (context.getCurrentRecord().getKey().isPresent()) {
                String messageKey = context.getCurrentRecord().getKey().get();
                context.getLogger().info(String.format(
                    "message TOPIC=%s; KEY=%s; VALUE=%s routing...",
                    context.getCurrentRecord().getTopicName().get(),
                    messageKey,
                    context.getCurrentRecord().getValue()
                ));
                if (messageKey.toUpperCase().startsWith("INVOICE")) {
                    outputTopicName = TOPIC_OUTPUT_INVOICE;
                }
                context.newOutputMessage(outputTopicName, Schema.BYTES)
                        .key(messageKey)
                        .value(((String) context.getCurrentRecord().getValue()).getBytes())
                        .send();
            }
        } catch (PulsarClientException e) {
            context.getLogger().error(e.toString());
        }
        return null;
    }

}
