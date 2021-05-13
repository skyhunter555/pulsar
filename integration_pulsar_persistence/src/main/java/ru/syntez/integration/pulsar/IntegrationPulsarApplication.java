package ru.syntez.integration.pulsar;

import org.apache.pulsar.client.api.PulsarClient;
import org.yaml.snakeyaml.Yaml;
import ru.syntez.integration.pulsar.config.PulsarConfig;
import ru.syntez.integration.pulsar.usecases.run.RunReceivingAfterReconnectUsecase;
import ru.syntez.integration.pulsar.usecases.run.RunSendingAfterReconnectUsecase;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Main class
 *
 * @author Skyhunter
 * @date 12.05.2021
 */
public class IntegrationPulsarApplication {

    private final static Logger LOG = Logger.getLogger(ru.syntez.integration.pulsar.IntegrationPulsarApplication.class.getName());
    private static PulsarConfig config;
    private static PulsarClient client;

    public static void main(String[] args) {

        Yaml yaml = new Yaml();
        //try( InputStream in = Files.newInputStream( Paths.get( args[ 0 ] ) ) ) {
        try (InputStream in = Files.newInputStream(Paths.get(IntegrationPulsarApplication.class.getResource("/application.yml").toURI()))) {
            config = yaml.loadAs(in, PulsarConfig.class);
            LOG.log(Level.INFO, config.toString());
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Error load PulsarConfig from resource", e);
            return;
        }

        try {

            client = PulsarClient.builder()
                    .serviceUrl(config.getBrokers())
                    .operationTimeout(config.getOperationTimeoutSeconds(), TimeUnit.SECONDS)
                    .connectionTimeout(config.getConnectTimeoutSeconds(),  TimeUnit.SECONDS)
                    .keepAliveInterval(60,  TimeUnit.SECONDS)
                    .maxLookupRequests(100)
                    .maxNumberOfRejectedRequestPerConnection(10)
                    .maxConcurrentLookupRequests(10)
                    .build();

            //кейс Автоматический реконнект клиентов при сбое узла кластера
            //LOG.info("******************** Запуск проверки отправки после реконнекта...");
            //RunSendingAfterReconnectUsecase.execute(config, client);

            LOG.info("******************** Запуск проверки приема после реконнекта...");
            RunReceivingAfterReconnectUsecase.execute(config, client);

            client.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
        LOG.info("Проверка всех кейсов завершена.");

    }

}