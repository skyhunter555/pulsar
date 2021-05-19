package ru.syntez.integration.pulsar.entities;

import lombok.Data;

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

    public ResultReport(String objectId, Boolean isProducer, Date startDateTime, Date endDateTime, Integer resultCount) {
        this.objectId = objectId;
        this.isProducer = isProducer;
        this.startDateTime = startDateTime;
        this.endDateTime = endDateTime;
        this.resultCount = resultCount;
    }
}
