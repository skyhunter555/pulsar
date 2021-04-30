package ru.syntez.integration.pulsar.utils;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

public class ResultOutput {

    private final static Logger LOG = Logger.getLogger(ru.syntez.integration.pulsar.IntegrationPulsarApplication.class.getName());

    public static void outputResult(Integer msgSent, Integer msgReceived, Map<String, Set<String>> consumerRecordSetMap) {
        LOG.info(String.format("Количество отправленных уникальных сообщений: %s", msgSent));
        LOG.info(String.format("Количество всех принятых сообщений: %s", msgReceived));
        Integer uniqueCount = 0;
        for (Map.Entry entry : consumerRecordSetMap.entrySet()) {
            Set consumerRecordSet = (Set) entry.getValue();
            uniqueCount = uniqueCount + consumerRecordSet.size();
            BigDecimal percent = (BigDecimal.valueOf(100).multiply(BigDecimal.valueOf(consumerRecordSet.size())))
                    .divide(BigDecimal.valueOf(msgReceived.longValue()), 3);
            LOG.info(String.format("Количество принятых уникальных сообщений на консюмере %s: %s (%s)",
                    entry.getKey(),
                    consumerRecordSet.size(),
                    percent
            ));
        }
        LOG.info(String.format("Количество принятых уникальных сообщений: %s", uniqueCount));
    }

}
