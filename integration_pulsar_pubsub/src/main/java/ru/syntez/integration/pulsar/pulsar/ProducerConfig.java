package ru.syntez.integration.pulsar.pulsar;

import lombok.Data;

@Data
public class ProducerConfig {

    private String  acks;
    private Integer retries;
    private Integer requestTimeoutMs;
    private Integer lingerMs;
    private Integer deliveryTimeoutMs;  //Задается в соответствии с формулой ( request.timeout.ms + linger.ms )

}
