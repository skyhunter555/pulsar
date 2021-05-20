package ru.syntez.integration.pulsar;

import org.apache.pulsar.client.api.PulsarClient;
import org.yaml.snakeyaml.Yaml;
import ru.syntez.integration.pulsar.config.PulsarConfig;
import ru.syntez.integration.pulsar.entities.DataSizeEnum;
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

            //1. кейс проверка обработки сообщений от 10 продюсеров
           // RunTestUsecase.execute(config, client, 10, DataSizeEnum.SIZE_1_KB);
           // RunTestUsecase.execute(config, client, 10, DataSizeEnum.SIZE_10_KB);
           // RunTestUsecase.execute(config, client, 10, DataSizeEnum.SIZE_100_KB);
           // RunTestUsecase.execute(config, client, 10, DataSizeEnum.SIZE_1_MB);

            //2. кейс проверка обработки сообщений от 20 продюсеров
           //RunTestUsecase.execute(config, client, 20, DataSizeEnum.SIZE_1_KB);
           //RunTestUsecase.execute(config, client, 20, DataSizeEnum.SIZE_10_KB);
           //RunTestUsecase.execute(config, client, 20, DataSizeEnum.SIZE_100_KB);
           //RunTestUsecase.execute(config, client, 20, DataSizeEnum.SIZE_1_MB);

            //3. кейс проверка обработки сообщений от 100 продюсеров
           // RunTestUsecase.execute(config, client, 100, DataSizeEnum.SIZE_1_KB);
           // RunTestUsecase.execute(config, client, 100, DataSizeEnum.SIZE_10_KB);
           // RunTestUsecase.execute(config, client, 100, DataSizeEnum.SIZE_100_KB);
           // RunTestUsecase.execute(config, client, 100, DataSizeEnum.SIZE_1_MB);

            //4. кейс проверка обработки сообщений от 200 продюсеров
            //RunTestUsecase.execute(config, client, 200, DataSizeEnum.SIZE_1_KB);
            //RunTestUsecase.execute(config, client, 200, DataSizeEnum.SIZE_10_KB);
            //RunTestUsecase.execute(config, client, 200, DataSizeEnum.SIZE_100_KB);
            //RunTestUsecase.execute(config, client, 200, DataSizeEnum.SIZE_1_MB);

            //5. кейс проверка обработки сообщений от 300 продюсеров
            //RunTestUsecase.execute(config, client, 300, DataSizeEnum.SIZE_1_KB);
            //RunTestUsecase.execute(config, client, 300, DataSizeEnum.SIZE_10_KB);
            //RunTestUsecase.execute(config, client, 300, DataSizeEnum.SIZE_100_KB);
            //RunTestUsecase.execute(config, client, 300, DataSizeEnum.SIZE_1_MB);


            //RunKafkaTestUsecase.execute(config, 10, DataSizeEnum.SIZE_1_KB);
            RunKafkaTestUsecase.execute(config, 10, DataSizeEnum.SIZE_10_KB);
            //RunKafkaTestUsecase.execute(config, 10, DataSizeEnum.SIZE_100_KB);
            //RunKafkaTestUsecase.execute(config, 10, DataSizeEnum.SIZE_1_MB);


            client.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
        LOG.info("Проверка всех кейсов завершена.");

    }

}
