package ru.syntez.integration.pulsar.functions;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.JacksonXmlModule;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.ser.ToXmlGenerator;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

import java.util.HashMap;
import java.util.Map;

public class TransformDemoFunction implements Function<String, Void> {

    private final String TOPIC_OUTPUT = "topic-output-demo";
    private ObjectMapper xmlMapper;

    private void initXMLMapper() {
        JacksonXmlModule xmlModule = new JacksonXmlModule();
        xmlModule.setDefaultUseWrapper(false);
        xmlMapper = new XmlMapper(xmlModule);
        ((XmlMapper) xmlMapper).enable(ToXmlGenerator.Feature.WRITE_XML_DECLARATION);
        xmlMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

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
