package ru.syntez.integration.pulsar;

import org.apache.pulsar.client.api.PulsarClient;
import org.yaml.snakeyaml.Yaml;
import ru.syntez.integration.pulsar.config.PulsarConfig;
import ru.syntez.integration.pulsar.entities.DataSizeEnum;
import ru.syntez.integration.pulsar.usecases.run.*;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;
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
            //  runPulsarTest(1);

              //2. кейс проверка обработки сообщений от 10 продюсеров
             // runPulsarTest(10);
//
            //  //3. кейс проверка обработки сообщений от 20 продюсеров
             // runPulsarTest(20);
//
            //  //4. кейс проверка обработки сообщений от 100 продюсеров
            //  runPulsarTest(100);
//
            //  //5. кейс проверка обработки сообщений от 200 продюсеров
            //  runPulsarTest(200);
//
            //  //6. кейс проверка обработки сообщений от 300 продюсеров
            //  runPulsarTest(300);

            // 7. кейс проверка обработки сообщений от 100 продюсеров не персистентный топик
            RunNonPersistantTestUsecase.execute(config, client, 100, DataSizeEnum.SIZE_1_KB);
            RunNonPersistantTestUsecase.execute(config, client, 100, DataSizeEnum.SIZE_10_KB);
            RunNonPersistantTestUsecase.execute(config, client, 100, DataSizeEnum.SIZE_100_KB);
            //RunNonPersistantTestUsecase.execute(config, client, 100, DataSizeEnum.SIZE_1_MB);

            //8. кейс проверка обработки сообщений KAFKA 10 продюсеров
            // RunKafkaTestUsecase.execute(config, config.getKafka().getProducerCount(), DataSizeEnum.SIZE_1_KB);
            // RunKafkaTestUsecase.execute(config, config.getKafka().getProducerCount(), DataSizeEnum.SIZE_1_KB);
            // RunKafkaTestUsecase.execute(config, config.getKafka().getProducerCount(), DataSizeEnum.SIZE_1_KB);
            // RunKafkaTestUsecase.execute(config, config.getKafka().getProducerCount(), DataSizeEnum.SIZE_10_KB);
            //RunKafkaTestUsecase.execute(config, config.getKafka().getProducerCount(), DataSizeEnum.SIZE_10_KB);
            //RunKafkaTestUsecase.execute(config, config.getKafka().getProducerCount(), DataSizeEnum.SIZE_10_KB);
            // RunKafkaTestUsecase.execute(config, config.getKafka().getProducerCount(), DataSizeEnum.SIZE_100_KB);
            // RunKafkaTestUsecase.execute(config, config.getKafka().getProducerCount(), DataSizeEnum.SIZE_100_KB);
            //  RunKafkaTestUsecase.execute(config, config.getKafka().getProducerCount(), DataSizeEnum.SIZE_100_KB);
           // RunKafkaTestUsecase.execute(config, config.getKafka().getProducerCount(), DataSizeEnum.SIZE_1_MB);


            client.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
        LOG.info("Проверка всех кейсов завершена.");

    }

    private static void runPulsarTest(Integer producerCount) throws ExecutionException, InterruptedException {
        RunTestUsecase.execute(config, client, producerCount, DataSizeEnum.SIZE_1_KB);
        RunTestUsecase.execute(config, client, producerCount, DataSizeEnum.SIZE_10_KB);
        RunTestUsecase.execute(config, client, producerCount, DataSizeEnum.SIZE_100_KB);
        //RunTestUsecase.execute(config, client, producerCount, DataSizeEnum.SIZE_1_MB);
    }

}