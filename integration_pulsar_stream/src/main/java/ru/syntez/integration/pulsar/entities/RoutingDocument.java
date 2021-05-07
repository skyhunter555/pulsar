package ru.syntez.integration.pulsar.entities;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;

/**
 * RoutingDocument model
 *
 * @author Skyhunter
 * @date 18.01.2021
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Data
public class RoutingDocument implements Serializable {

    private DocumentTypeEnum docType;
    private int docId;
    private int amount;

}
