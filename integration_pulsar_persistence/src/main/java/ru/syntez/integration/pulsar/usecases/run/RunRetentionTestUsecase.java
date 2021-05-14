package ru.syntez.integration.pulsar.usecases.run;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import ru.syntez.integration.pulsar.config.PulsarConfig;
import ru.syntez.integration.pulsar.entities.SubscriptionNameEnum;
import ru.syntez.integration.pulsar.scenarios.ProducerTestScenario;
import ru.syntez.integration.pulsar.scenarios.ProducerWithoutInterval;
import ru.syntez.integration.pulsar.usecases.ResultOutputUsecase;
import ru.syntez.integration.pulsar.usecases.StartConsumerUsecase;
import ru.syntez.integration.pulsar.usecases.create.ConsumerCreatorUsecase;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Запуск обработки для проверки персистентного топика
 *
 *  @author Skyhunter
 *  @date 14.05.2021
 */
public class RunRetentionTestUsecase {

    private static AtomicInteger msgSentCounter = new AtomicInteger(0);
    private static Map recordSetMap = new ConcurrentHashMap<>();

    public static void execute(
            PulsarConfig config,
            PulsarClient client
    ) throws InterruptedException {

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.execute(() -> {
            ProducerTestScenario testScenario = new ProducerWithoutInterval(client, config);
            msgSentCounter.set(testScenario.run(config.getTopicName()));
        });

        executorService.awaitTermination(30, TimeUnit.SECONDS);

        executorService.execute(() -> {
            try {
                String consumerId = "persistent";
                Consumer consumer = ConsumerCreatorUsecase.execute(
                        client, config, config.getTopicName(), consumerId,
                        String.format("%s_%s", SubscriptionNameEnum.SUBSCRIPTION_KEY_NAME.getCode(), consumerId));
                recordSetMap.put(consumerId, StartConsumerUsecase.execute(consumer, config.getRecordLogOutputEnabled(), 0, true));
                consumer.close();
            } catch (PulsarClientException | InterruptedException e) {
                e.printStackTrace();
            }
        });
        executorService.awaitTermination(10, TimeUnit.SECONDS);

        ResultOutputUsecase.execute(msgSentCounter.get(), recordSetMap);
    }
}
