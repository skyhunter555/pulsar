package ru.syntez.integration.pulsar.usecases;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.JacksonXmlModule;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import ru.syntez.integration.pulsar.entities.RoutingDocument;

/**
 *  Сериализация документа
 *
 *  @author Skyhunter
 *  @date 03.05.2021
 */
public class SerializeDocumentUsecase {

    private static ObjectMapper xmlMapper() {
        JacksonXmlModule xmlModule = new JacksonXmlModule();
        xmlModule.setDefaultUseWrapper(false);
        ObjectMapper xmlMapper = new XmlMapper(xmlModule);
        xmlMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        return new XmlMapper(xmlModule);
    }

    public static byte[] execute(RoutingDocument document) throws JsonProcessingException {
        return xmlMapper().writeValueAsString(document).getBytes();
    }

}
