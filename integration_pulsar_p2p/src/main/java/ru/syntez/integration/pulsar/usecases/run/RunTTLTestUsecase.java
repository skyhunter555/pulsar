package ru.syntez.integration.pulsar.usecases.run;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import ru.syntez.integration.pulsar.config.PulsarConfig;
import ru.syntez.integration.pulsar.entities.SubscriptionNameEnum;
import ru.syntez.integration.pulsar.scenarios.ProducerTestScenario;
import ru.syntez.integration.pulsar.scenarios.ProducerWithKeys;
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
 * Запуск обработки через время после записи для проверки TTL
 *
 *  @author Skyhunter
 *  @date 05.05.2021
 */
public class RunTTLTestUsecase {
    private static AtomicInteger msgSentCounter = new AtomicInteger(0);
    private static Map recordSetMap = new ConcurrentHashMap<>();

    public static void execute(
            PulsarConfig config,
            PulsarClient client
    ) throws InterruptedException {

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.execute(() -> {
            ProducerTestScenario testScenario = new ProducerWithKeys(client, config);
            msgSentCounter.set(testScenario.run(config.getTopicName()));
        });
        executorService.awaitTermination(config.getTimeoutBeforeConsume(), TimeUnit.MINUTES);

        executorService.execute(() -> {
            try {
                String consumerId = "ttl";
                Consumer consumer = ConsumerCreatorUsecase.execute(
                        client, config, config.getTopicName(), consumerId,
                        String.format("%s_%s", SubscriptionNameEnum.SUBSCRIPTION_KEY_NAME.getCode(), consumerId),
                        true, true);
                recordSetMap.put(consumerId, StartConsumerUsecase.execute(consumer, config.getRecordLogOutputEnabled()));
                consumer.close();
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }
        });
        executorService.shutdown();
        //Минуты должно хватить на обработку всех сообщений
        executorService.awaitTermination(1, TimeUnit.MINUTES);
        ResultOutputUsecase.execute(msgSentCounter.get(), recordSetMap);
    }
}
