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
import ru.syntez.integration.pulsar.processor.entities.OutputDocumentExt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//TODO window
public class AggregationByTimeDemoFunction implements Function<String, Void> {

    private final String TOPIC_OUTPUT = "topic-output-demo";
    private ObjectMapper xmlMapper;
    private final List<OutputDocumentExt> outputDocumentExtList = new ArrayList<>();

    private void initXMLMapper() {
        JacksonXmlModule xmlModule = new JacksonXmlModule();
        xmlModule.setDefaultUseWrapper(false);
        xmlMapper = new XmlMapper(xmlModule);
        ((XmlMapper) xmlMapper).enable(ToXmlGenerator.Feature.WRITE_XML_DECLARATION);
        xmlMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public Void process(String input, Context context) {
        //context.getLogger().info("RoutingByKeyDemoFunction check!!! context:" + context.toString());

        OutputDocumentExt outputDocumentExt;
        try {
            outputDocumentExt = xmlMapper.readValue((String) context.getCurrentRecord().getValue(), OutputDocumentExt.class);
        } catch (Exception ex) {
            context.getLogger().error("Failed to read XML string: " + ex.getMessage());
            return null;
        }

        try {
            if (context.getCurrentRecord().getKey().isPresent()) {
                String messageKey = context.getCurrentRecord().getKey().get();
                context.newOutputMessage(TOPIC_OUTPUT, Schema.BYTES)
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
