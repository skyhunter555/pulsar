package ru.syntez.integration.pulsar.usecases;

import ru.syntez.integration.pulsar.entities.ResultReport;

import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

/**
 * Вывод финального результата
 *
 * @author Skyhunter
 * @date 20.05.2021
 */
public class ResultOutputUsecase {

    private final static Logger LOG = Logger.getLogger(ru.syntez.integration.pulsar.IntegrationPulsarApplication.class.getName());

    public static void execute(List<ResultReport> promiseList, int producerCount, int timeOutSeconds) {

        Integer msgSent = promiseList.stream().filter(ResultReport::getIsProducer).mapToInt(ResultReport::getResultCount).sum();
        Integer msgReceived = promiseList.stream().filter(value -> !value.getIsProducer()).mapToInt(ResultReport::getResultCount).sum();
        long startDateTimeMin = promiseList.stream().filter(ResultReport::getIsProducer).mapToLong(value -> value.getStartDateTime().getTime()).min().getAsLong();
        long endDateTimeMax = promiseList.stream().filter(value -> !value.getIsProducer()).mapToLong(value -> value.getEndDateTime().getTime()).max().getAsLong();

        LOG.info(String.format("Обработка завершена. Результаты для %s продюсеров: ", producerCount));
        LOG.info(String.format("Количество отправленных сообщений: %s", msgSent));
        LOG.info(String.format("Количество всех принятых сообщений: %s", msgReceived));
        LOG.info(String.format("Начало отправки сообщений: %s", new Date(startDateTimeMin)));
        LOG.info(String.format("Завершение отправки сообщений: %s", new Date(endDateTimeMax)));
        LOG.info(String.format("Общее время обработки сообщений: %s ms", endDateTimeMax - startDateTimeMin));
    }

}
