package ru.syntez.integration.pulsar.entities;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.util.List;

/**
 * ComposeDocument model for outputDocumentListQueue
 *
 * @author Skyhunter
 * @date 30.04.2021
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Data
public class ComposeDocument {
    private int composeId;
    private String composeLabel;
    private List<OrderDocument> orderDocuments;
    private List<InvoiceDocument> invoiceDocuments;
}
