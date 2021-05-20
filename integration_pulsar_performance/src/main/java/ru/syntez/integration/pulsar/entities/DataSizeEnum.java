package ru.syntez.integration.pulsar.entities;

/**
 * DataSize enum 1 kb, 10 kb, 100 kb, 1 Mb
 *
 * @author Skyhunter
 * @date 19.05.2021
 */
public enum DataSizeEnum {

    SIZE_1_KB(1,"1 kb"),
    SIZE_10_KB(10, "10 kb"),
    SIZE_100_KB(100, "100 kb"),
    SIZE_1_MB(1024, "1 Mb"),
    SIZE_5_MB(5120, "5 Mb");

    private final Integer factor;
    private final String description;

    DataSizeEnum(Integer factor, String description) {
        this.factor = factor;
        this.description = description;
    }

    public String getDescription() {
        return description;
    }

    public Integer getFactor() {
        return factor;
    }
}