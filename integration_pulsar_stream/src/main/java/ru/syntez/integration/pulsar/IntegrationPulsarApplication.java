package ru.syntez.integration.pulsar;

import org.apache.pulsar.client.api.*;
import org.yaml.snakeyaml.Yaml;
import ru.syntez.integration.pulsar.usecases.run.RunAggregationByCountUsecase;
import ru.syntez.integration.pulsar.usecases.run.RunAggregationByTimeUsecase;
import ru.syntez.integration.pulsar.usecases.run.RunTransformFunctionTestUsecase;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Main class
 *
 * @author Skyhunter
 * @date 11.05.2021
 */
public class IntegrationPulsarApplication {

    private final static Logger LOG = Logger.getLogger(ru.syntez.integration.pulsar.IntegrationPulsarApplication.class.getName());
    private static ru.syntez.integration.pulsar.pulsar.PulsarConfig config;
    private static PulsarClient client;

    public static void main(String[] args) {

        Yaml yaml = new Yaml();
        //try( InputStream in = Files.newInputStream( Paths.get( args[ 0 ] ) ) ) {
        try (InputStream in = Files.newInputStream(Paths.get(IntegrationPulsarApplication.class.getResource("/application.yml").toURI()))) {
            config = yaml.loadAs(in, ru.syntez.integration.pulsar.pulsar.PulsarConfig.class);
            LOG.log(Level.INFO, config.toString());
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Error load PulsarConfig from resource", e);
            return;
        }

        try {
            client = PulsarClient.builder()
                    .serviceUrl(config.getBrokers())
                    .build();

            //кейс Применение преобразования формата сообщения в реальном времени в потоковом режиме.
            LOG.info("******************** Запуск проверки преобразования формата сообщения...");
            RunTransformFunctionTestUsecase.execute(config, client);

            //кейс Агрегация по 100 сообщений из одного топика с целью получения единого сообщения содержащего данные всех переданных сообщений.
            //Агрегация сообщений из разных топиков.
            LOG.info("******************** Запуск проверки агрегации по 100 сообщений...");
            RunAggregationByCountUsecase.execute(config, client);

            //кейс Событие в формате JSON содержит поле целое числовое поле amount
            //Рассчитать сумму по полю amount за последнюю минуту
            LOG.info("******************** Запуск проверки агрегации за последнюю минуту...");
            RunAggregationByTimeUsecase.execute(config, client);

            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        LOG.info("Проверка всех кейсов завершена.");

    }

}
