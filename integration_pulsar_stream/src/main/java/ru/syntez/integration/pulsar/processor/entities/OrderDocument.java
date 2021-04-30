package ru.syntez.integration.pulsar.processor.entities;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

/**
 * OrderDocument
 *
 * @author Skyhunter
 * @date 30.04.2021
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Data
public class OrderDocument {
    private int orderId;
    private String orderNumber;
}
