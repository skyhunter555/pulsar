package ru.syntez.integration.pulsar.usecases.run;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import ru.syntez.integration.pulsar.config.PulsarConfig;
import ru.syntez.integration.pulsar.entities.SubscriptionNameEnum;
import ru.syntez.integration.pulsar.scenarios.ProducerTestScenario;
import ru.syntez.integration.pulsar.scenarios.ProducerWithKeys;
import ru.syntez.integration.pulsar.scenarios.ProducerWithVersions;
import ru.syntez.integration.pulsar.scenarios.ProducerWithoutKeys;
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
 * Проверка сценариев по гарантиям доставки
 *
 *  @author Skyhunter
 *  @date 05.05.2021
 */
public class RunGuarantiesTestUsecase {
    private static AtomicInteger msgSentCounter = new AtomicInteger(0);
    private static Map recordSetMap = new ConcurrentHashMap();
    private static int consumerCount = 3;

    public static void execute(
            PulsarConfig config,
            PulsarClient client,
            Boolean withKeys,
            Boolean withVersions,
            String topicName
    ) throws InterruptedException {

        Runnable producerScenario = () -> {
            ProducerTestScenario testScenario;
            if (withKeys && withVersions) testScenario = new ProducerWithVersions(client, config);
            else if (!withKeys) testScenario = new ProducerWithoutKeys(client, config);
            else  testScenario = new ProducerWithKeys(client, config);
            msgSentCounter.set(testScenario.run(topicName));
        };

        ExecutorService executorService = Executors.newFixedThreadPool(consumerCount + 1);
        executorService.execute(producerScenario);

        for (int i = 0; i < consumerCount; i++) {
            String consumerId = Integer.toString(i + 1);
            executorService.execute(() -> {
                Consumer consumer = null;
                try {
                    consumer = createAndStartConsumer(client, consumerId, withKeys, topicName);
                } catch (PulsarClientException e) {
                    e.printStackTrace();
                } finally {
                    if (consumer != null) {
                        try {
                            consumer.close();
                        } catch (PulsarClientException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }
        executorService.shutdown();
        //Минуты должно хватить на обработку всех сообщений
        executorService.awaitTermination(1, TimeUnit.MINUTES);
        ResultOutputUsecase.execute(msgSentCounter.get(), recordSetMap);
    }

    private static Consumer createAndStartConsumer(PulsarClient client, String consumerId, Boolean withKeys, String topicName) throws PulsarClientException {
        String subscriptionName;
        if (withKeys) {
            subscriptionName = SubscriptionNameEnum.SUBSCRIPTION_KEY_NAME.getCode();
        } else {
            subscriptionName = SubscriptionNameEnum.SUBSCRIPTION_NAME.getCode();
        }
        Consumer<byte[]> consumer = ConsumerCreatorUsecase.execute(
                client,
                topicName,
                consumerId,
                subscriptionName);
        recordSetMap.put(consumerId, StartConsumerUsecase.execute(consumer, false));

        return consumer;
    }
}
