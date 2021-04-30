package ru.syntez.integration.pulsar.functions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.JacksonXmlModule;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.ser.ToXmlGenerator;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import ru.syntez.integration.pulsar.processor.MapStructConverter;
import ru.syntez.integration.pulsar.processor.entities.OutputDocumentExt;

public class TransformDemoFunction implements Function<String, Void> {

    private final String TOPIC_OUTPUT_ORDER = "topic-output-order-demo";
    private final String TOPIC_OUTPUT_INVOICE = "topic-output-invoice-demo";
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

        initXMLMapper();

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
                context.getLogger().info(String.format(
                        "message TOPIC=%s; KEY=%s; VALUE=%s routing...",
                        context.getCurrentRecord().getTopicName().get(),
                        messageKey,
                        context.getCurrentRecord().getValue()
                ));
                if (outputDocumentExt.getDocumentType().equals("order")) {
                    String orderDocument = xmlMapper.writeValueAsString(MapStructConverter.MAPPER.convertOrder(outputDocumentExt));
                    context.newOutputMessage(TOPIC_OUTPUT_ORDER, Schema.BYTES)
                            .key(messageKey)
                            .value(orderDocument.getBytes())
                            .send();
                } else if (outputDocumentExt.getDocumentType().equals("invoice")) {
                    String invoiceDocument = xmlMapper.writeValueAsString(MapStructConverter.MAPPER.convertInvoice(outputDocumentExt));
                    context.newOutputMessage(TOPIC_OUTPUT_INVOICE, Schema.BYTES)
                            .key(messageKey)
                            .value(invoiceDocument.getBytes())
                            .send();
                }
            }
        } catch (PulsarClientException | JsonProcessingException e) {
            context.getLogger().error(e.toString());
        }
        return null;
    }

}


