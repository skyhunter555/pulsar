package ru.syntez.integration.pulsar.usecases;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.JacksonXmlModule;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import ru.syntez.integration.pulsar.entities.RoutingDocument;

import java.io.IOException;

/**
 *  Десериализация документа
 *
 *  @author Skyhunter
 *  @date 03.05.2021
 */
public class DeserializeDocumentUsecase {

    private static ObjectMapper xmlMapper() {
        JacksonXmlModule xmlModule = new JacksonXmlModule();
        xmlModule.setDefaultUseWrapper(false);
        ObjectMapper xmlMapper = new XmlMapper(xmlModule);
        xmlMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        return new XmlMapper(xmlModule);
    }

    public static RoutingDocument execute(byte[] document) throws IOException {
        return xmlMapper().readValue(document, RoutingDocument.class);
    }

}
