package ru.syntez.integration.pulsar.usecases;

import ru.syntez.integration.pulsar.entities.ResultReport;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Logger;

/**
 * Вывод финального результата
 *
 * @author Skyhunter
 * @date 05.05.2021
 */
public class ResultOutputUsecase {

    private final static Logger LOG = Logger.getLogger(ru.syntez.integration.pulsar.IntegrationPulsarApplication.class.getName());

    public static void execute(List<ResultReport> promiseList) {

        Integer msgSent = 0; //= promiseList.stream().filter(ResultReport::getIsProducer).mapToInt(ResultReport::getResultCount).sum();
        Integer msgReceived = 0; //= promiseList.stream().filter(value -> !value.getIsProducer()).mapToInt(ResultReport::getResultCount).sum();
        long startDateTimeMin = new Date().getTime(); // = promiseList.stream().filter(ResultReport::getIsProducer).mapToLong(value -> value.getStartDateTime().getTime()).min().getAsLong();
        long endDateTimeMax = 0L; // = promiseList.stream().filter(value -> !value.getIsProducer()).mapToLong(value -> value.getEndDateTime().getTime()).max().getAsLong();

        for (ResultReport result: promiseList) {
            if (result.getIsProducer()) {
                msgSent = msgSent + result.getResultCount();
                if (result.getStartDateTime().getTime() < startDateTimeMin) {
                    startDateTimeMin = result.getStartDateTime().getTime();
                }
            } else {
                msgReceived = msgReceived + result.getResultCount();
                if (result.getEndDateTime().getTime() > endDateTimeMax) {
                    endDateTimeMax = result.getEndDateTime().getTime();
                }
            }
        }
        LOG.info("Проверка завершена. Результаты: ");
        LOG.info(String.format("Количество отправленных сообщений: %s", msgSent));
        LOG.info(String.format("Количество всех принятых сообщений: %s", msgReceived));
        LOG.info(String.format("Начало отправки сообщений: %s", new Date(startDateTimeMin)));
        LOG.info(String.format("Завершение отправки сообщений: %s", new Date(endDateTimeMax)));
        LOG.info(String.format("Общее время обработки сообщений: %s ms", endDateTimeMax - startDateTimeMin));
    }

}
