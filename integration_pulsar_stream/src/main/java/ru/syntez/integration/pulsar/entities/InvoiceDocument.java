package ru.syntez.integration.pulsar.entities;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

/**
 * InvoiceDocument
 *
 * @author Skyhunter
 * @date 30.04.2021
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Data
public class InvoiceDocument {
    private int invoiceId;
    private String invoiceNumber;
}
