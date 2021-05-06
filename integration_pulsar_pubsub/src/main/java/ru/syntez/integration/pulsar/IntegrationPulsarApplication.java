package ru.syntez.integration.pulsar;

import org.apache.pulsar.client.api.PulsarClient;
import org.yaml.snakeyaml.Yaml;
import ru.syntez.integration.pulsar.config.PulsarConfig;
import ru.syntez.integration.pulsar.usecases.run.RunGuarantiesTestUsecase;
import ru.syntez.integration.pulsar.usecases.run.RunTTLTestUsecase;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Main class
 *
 * @author Skyhunter
 * @date 30.04.2021
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
                    .build();

            //кейс ATLEAST_ONCE
            LOG.info("******************** Запуск проверки гарантии доставки ATLEAST_ONCE с ключами...");
            RunGuarantiesTestUsecase.execute(config, client, true, false, config.getTopicAtleastName(), false, true);

            //кейс ATMOST_ONCE
            LOG.info("******************** Запуск проверки гарантии доставки ATMOST_ONCE с ключами...");
            RunGuarantiesTestUsecase.execute(config, client,  true, false, config.getTopicAtmostName(), false, false);

            //кейс EFFECTIVELY_ONCE
            LOG.info("******************** Запуск проверки гарантии доставки EFFECTIVELY_ONCE + дедупликация без сжатия...");
            RunGuarantiesTestUsecase.execute(config, client,  true, true, config.getTopicEffectivelyName(), false, true);
            LOG.info("******************** Запуск проверки гарантии доставки EFFECTIVELY_ONCE + дедупликация с сжатием...");
            RunGuarantiesTestUsecase.execute(config, client,  true, true, config.getTopicEffectivelyName(), true, true);

            //кейс TTL
            LOG.info("******************** Запуск проверки TTL...");
            RunTTLTestUsecase.execute(config, client);

            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        LOG.info("Проверка всех кейсов завершена.");

    }

}
