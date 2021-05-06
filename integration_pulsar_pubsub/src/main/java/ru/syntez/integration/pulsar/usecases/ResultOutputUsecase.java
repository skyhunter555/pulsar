package ru.syntez.integration.pulsar.usecases;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Вывод финального результата
 *
 * @author Skyhunter
 * @date 05.05.2021
 */
public class ResultOutputUsecase {

    private final static Logger LOG = Logger.getLogger(ru.syntez.integration.pulsar.IntegrationPulsarApplication.class.getName());

    public static void execute(Integer msgSent, Map<String, Set<String>> consumerRecordSetMap) {

        long msgReceived = consumerRecordSetMap.values().stream().mapToLong(Set::size).sum();

        LOG.info("Проверка завершена. Результаты: ");
        LOG.info(String.format("Количество отправленных уникальных сообщений: %s", msgSent));
        LOG.info(String.format("Количество всех принятых сообщений: %s", msgReceived));

        Set uniqueMessage = new HashSet();

        for (Map.Entry entry : consumerRecordSetMap.entrySet()) {
            Set consumerRecordSet = (Set) entry.getValue();
            uniqueMessage = consumerRecordSet;
            BigDecimal percent = BigDecimal.ZERO;
            if (msgReceived > 0) {
                percent = (BigDecimal.valueOf(100).multiply(BigDecimal.valueOf(consumerRecordSet.size())))
                        .divide(BigDecimal.valueOf(msgReceived), 3);
            }
            LOG.info(String.format("Количество принятых уникальных сообщений на консюмере %s: %s (%s)",
                    entry.getKey(),
                    consumerRecordSet.size(),
                    percent
            ));
        }
        LOG.info(String.format("Количество принятых уникальных сообщений: %s", uniqueMessage.size()));
    }

}
