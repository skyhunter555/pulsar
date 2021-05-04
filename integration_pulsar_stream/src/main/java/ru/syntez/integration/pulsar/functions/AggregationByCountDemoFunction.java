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
import ru.syntez.integration.pulsar.entities.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class AggregationByCountDemoFunction implements Function<String, Void> {

    private final String TOPIC_OUTPUT = "topic-output-demo";
    private final String DOCUMENT_COUNTER = "documents";
    private final String COMPOSE_DOCUMENT_COUNTER = "compose_documents";
    private final int messageCount = 100;

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
        //context.getLogger().info("RoutingByKeyDemoFunction check!!! context:" + context.toString());
        initXMLMapper();
        OutputDocumentExt outputDocumentExt;
        try {
            outputDocumentExt = xmlMapper.readValue((String) context.getCurrentRecord().getValue(), OutputDocumentExt.class);
        } catch (Exception ex) {
            context.getLogger().error("Failed to read XML string: " + ex.getMessage());
            return null;
        }
        context.incrCounter(DOCUMENT_COUNTER, 1);

        Long documentCounter = context.getCounter(DOCUMENT_COUNTER);
        try {
            putToContext(context, outputDocumentExt, documentCounter);
        } catch (JsonProcessingException e) {
            context.getLogger().error(e.toString());
        }

        if (documentCounter == messageCount) {
            try {
                context.incrCounter(COMPOSE_DOCUMENT_COUNTER, 1);
                sendComposeDocument(context);
             } catch (PulsarClientException | JsonProcessingException e) {
                context.getLogger().error(e.toString());
            }
            context.incrCounter(DOCUMENT_COUNTER, -messageCount);
        }
        return null;
    }

    private void sendComposeDocument(Context context) throws PulsarClientException, JsonProcessingException {

        Long composeId = context.getCounter(COMPOSE_DOCUMENT_COUNTER);

        context.getLogger().info(String.format("Try to create ComposeDocument with id=%s from context...", composeId));

        ComposeDocument composeDocument = new ComposeDocument();
        composeDocument.setComposeId(composeId);
        composeDocument.setComposeLabel(COMPOSE_DOCUMENT_COUNTER);
        composeDocument.setOrderDocuments(getOrdersFromContext(context));
        composeDocument.setInvoiceDocuments(getInvoicesFromContext(context));
        context.newOutputMessage(TOPIC_OUTPUT, Schema.BYTES)
                .value(xmlMapper.writeValueAsString(composeDocument).getBytes())
                .send();
    }

    private List<InvoiceDocument> getInvoicesFromContext(Context context) {
        List<InvoiceDocument> documentList = new ArrayList<>();
        for (int index = 0; index < messageCount; index++) {
            String stateId = String.format("%s_%s", DocumentTypeEnum.invoice.name(), index);
            ByteBuffer state = context.getState(stateId);
            if (state != null) {
                try {
                    InvoiceDocument document = xmlMapper.readValue(state.array(), InvoiceDocument.class);
                    documentList.add(document);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return documentList;
    }

    private List<OrderDocument> getOrdersFromContext(Context context) {
        List<OrderDocument> documentList = new ArrayList<>();
        for (int index = 0; index < messageCount; index++) {
            String stateId = String.format("%s_%s", DocumentTypeEnum.order.name(), index);
            ByteBuffer state = context.getState(stateId);
            if (state != null) {
                try {
                    OrderDocument document = xmlMapper.readValue(state.array(), OrderDocument.class);
                    documentList.add(document);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return documentList;
    }

    private void putToContext(Context context, OutputDocumentExt outputDocumentExt, Long documentCounter) throws JsonProcessingException {
        if (!context.getCurrentRecord().getKey().isPresent()) {
            return;
        }
        String messageKey = context.getCurrentRecord().getKey().get();
        byte[] documentBody;
        if (outputDocumentExt.getDocumentType().equals(DocumentTypeEnum.order)) {
            documentBody = xmlMapper.writeValueAsString(MapStructConverter.MAPPER.convertOrder(outputDocumentExt)).getBytes();
        } else if (outputDocumentExt.getDocumentType().equals(DocumentTypeEnum.invoice)) {
            documentBody = xmlMapper.writeValueAsString(MapStructConverter.MAPPER.convertInvoice(outputDocumentExt)).getBytes();
        } else {
            return;
        }

        String stateId = String.format("%s_%s", outputDocumentExt.getDocumentType(), documentCounter);
        context.putState(stateId, ByteBuffer.wrap(documentBody));

        context.getLogger().info(String.format(
                "message TOPIC=%s; KEY=%s; VALUE=%s putToContext...",
                context.getCurrentRecord().getTopicName().get(),
                messageKey,
                context.getCurrentRecord().getValue()
        ));
    }

}
