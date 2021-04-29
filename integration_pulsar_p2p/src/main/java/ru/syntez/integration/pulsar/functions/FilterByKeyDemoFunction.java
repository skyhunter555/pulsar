package ru.syntez.integration.pulsar.functions;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

import java.util.HashMap;
import java.util.Map;

public class FilterByKeyDemoFunction implements Function<String, Void> {

    private final String TOPIC_OUTPUT = "topic-output-demo";

    @Override
    public Void process(String input, Context context) {

        Map<String, String> properties = new HashMap<>();
        properties.put("input_topic", context.getCurrentRecord().getTopicName().get());
        properties.putAll(context.getCurrentRecord().getProperties());
        try {
            if (context.getCurrentRecord().getKey().isPresent()) {
                String messageKey = context.getCurrentRecord().getKey().get();
                context.getLogger().info(
                        "Topic message with key \"" + messageKey + "\" and message value \"" + context.getCurrentRecord().getValue() + "\" routing"
                );
                if (messageKey.toUpperCase().startsWith("ORDER")) {
                    context.newOutputMessage(TOPIC_OUTPUT, Schema.BYTES)
                            .key(messageKey)
                            .value(((String) context.getCurrentRecord().getValue()).getBytes())
                            .send();
                }
            }
        } catch (PulsarClientException e) {
            context.getLogger().error(e.toString());
        }
        return null;
    }

}
