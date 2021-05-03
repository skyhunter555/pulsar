package ru.syntez.integration.pulsar.functions;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.Mappings;
import org.mapstruct.factory.Mappers;
import ru.syntez.integration.pulsar.entities.InvoiceDocument;
import ru.syntez.integration.pulsar.entities.OrderDocument;
import ru.syntez.integration.pulsar.entities.OutputDocumentExt;

/**
 * MapStructConverter
 *
 * @author Skyhunter
 * @date 30.04.2021
 */
@Mapper
public interface MapStructConverter {

     MapStructConverter MAPPER = Mappers.getMapper(MapStructConverter.class);

     @Mappings({
             @Mapping(source="documentId",     target="orderId"),
             @Mapping(source="documentNumber", target="orderNumber")
     })
     OrderDocument convertOrder(OutputDocumentExt outputDocumentExt);

     @Mappings({
             @Mapping(source="documentId",     target="invoiceId"),
             @Mapping(source="documentNumber", target="invoiceNumber")
     })
     InvoiceDocument convertInvoice(OutputDocumentExt outputDocumentExt);

}
