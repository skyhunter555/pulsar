package ru.syntez.integration.pulsar.entities;

import lombok.Data;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.Date;

/**
 * RoutingDocument model
 *
 * @author Skyhunter
 * @date 19.05.2021
 */
@XmlRootElement(name = "routingDocument")
@XmlAccessorType(XmlAccessType.FIELD)
@Data
public class RoutingDocument implements Serializable {

    private DocumentTypeEnum docType;
    private int docId;
    private String docData;
    private long docTime;
    /**
     * Для кейсов фильтрации и маршрутизации:
     * В зависимости от идентификатора тип документа может быть разным
     * @param id
     * @return
     */
    public static RoutingDocument createAny(int id, String docData) {
        final RoutingDocument document = new RoutingDocument();
        document.setDocId(id);
        document.setDocData(docData);
        document.setDocTime(new Date().getTime());
        if ((id % 2) == 0) {
            document.setDocType(DocumentTypeEnum.order);
        } else {
            document.setDocType(DocumentTypeEnum.invoice);
        }
        return document;
    }

}
