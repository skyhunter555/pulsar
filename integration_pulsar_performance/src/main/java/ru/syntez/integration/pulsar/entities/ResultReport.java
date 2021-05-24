package ru.syntez.integration.pulsar.entities;

import lombok.Data;

import java.math.BigDecimal;
import java.util.Date;

/**
 * ResultReport model
 *
 * @author Skyhunter
 * @date 19.05.2021
 */
@Data
public class ResultReport {

    private String objectId;
    private Boolean isProducer;
    private Date startDateTime;
    private Date endDateTime;
    private Integer resultCount;
    private BigDecimal publishLatency;
    private BigDecimal endToEndLatency;

    public ResultReport(String objectId, Boolean isProducer,
                        Date startDateTime, Date endDateTime,
                        Integer resultCount,
                        BigDecimal publishLatency, BigDecimal endToEndLatency
    ) {
        this.objectId = objectId;
        this.isProducer = isProducer;
        this.startDateTime = startDateTime;
        this.endDateTime = endDateTime;
        this.resultCount = resultCount;
        this.publishLatency = publishLatency;
        this.endToEndLatency = endToEndLatency;
    }
}
