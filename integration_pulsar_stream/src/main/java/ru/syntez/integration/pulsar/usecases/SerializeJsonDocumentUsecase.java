package ru.syntez.integration.pulsar.usecases;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import ru.syntez.integration.pulsar.entities.RoutingDocument;

/**
 *  Сериализация документа
 *
 *  @author Skyhunter
 *  @date 03.05.2021
 */
public class SerializeJsonDocumentUsecase {

    private static ObjectMapper jsonMapper() {
        return new ObjectMapper();
    }

    public static byte[] execute(RoutingDocument document) throws JsonProcessingException {
        return jsonMapper().writeValueAsString(document).getBytes();
    }

}
