package ru.syntez.integration.pulsar;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.yaml.snakeyaml.Yaml;
import ru.syntez.integration.pulsar.config.PulsarConfig;
import ru.syntez.integration.pulsar.usecases.run.*;

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
                    .serviceUrl(String.format("%s:%s", config.getBrokersUrl(), config.getBrokerPort()))
                    .operationTimeout(config.getOperationTimeoutSeconds(), TimeUnit.SECONDS)
                    .connectionTimeout(config.getConnectTimeoutSeconds(),  TimeUnit.SECONDS)
                    .build();

            PulsarAdmin admin = createPulsarAdmin(String.format("%s:%s", config.getAdminUrl(), config.getAdminPort()));

            //кейс Автоматический реконнект клиентов при сбое узла кластера
            LOG.info("******************** Запуск проверки отправки после реконнекта...");
            RunSendingAfterReconnectUsecase.execute(config, client);

            LOG.info("******************** Запуск проверки приема после реконнекта...");
            RunReceivingAfterReconnectUsecase.execute(config, client);

            LOG.info("******************** Запуск проверки не персистентого топика...");
            RunNonPersistentTestUsecase.execute(config, client);

            LOG.info("******************** Запуск обработки проверки при изменении брокеров...");
            RunClusterBrokerTestUsecase.execute(config, client, admin);

            LOG.info("******************** Запуск обработки проверки персистентого топика...");
            RunRetentionTestUsecase.execute(config, client);

            client.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
        LOG.info("Проверка всех кейсов завершена.");

    }

    private static PulsarAdmin createPulsarAdmin(String url) {
        try {
            LOG.info(String.format("Create Pulsar Admin instance. url=%s", url));
            PulsarAdminBuilder pulsarAdminBuilder = PulsarAdmin.builder();
            pulsarAdminBuilder.serviceHttpUrl(url);
            return pulsarAdminBuilder.build();
        } catch (PulsarClientException e) {
            LOG.info(String.format("Failed to create Pulsar Admin instance. url=%s", url));
        }
        return null;
    }
}
