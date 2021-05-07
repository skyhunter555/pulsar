package ru.syntez.integration.pulsar.usecases.run;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import ru.syntez.integration.pulsar.entities.SubscriptionNameEnum;
import ru.syntez.integration.pulsar.pulsar.ConsumerCreator;
import ru.syntez.integration.pulsar.pulsar.PulsarConfig;
import ru.syntez.integration.pulsar.usecases.ResultOutputUsecase;
import ru.syntez.integration.pulsar.usecases.StartConsumerUsecase;
import ru.syntez.integration.pulsar.usecases.StartProducerUsecase;
import ru.syntez.integration.pulsar.usecases.StartProducerWindowUsecase;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Запуск обработки сообщений с агрегацией по времени
 */
public class RunAggregationByTimeUsecase {

    private static AtomicInteger msgSentCounter = new AtomicInteger(0);
    private static Map recordSetMap = new ConcurrentHashMap<>();

    public static void execute(
            PulsarConfig config,
            PulsarClient client
    ) throws InterruptedException {

        ExecutorService executorService = Executors.newFixedThreadPool(3);
        executorService.execute(() -> {
            try {
                msgSentCounter.set(StartProducerWindowUsecase.execute(client, config.getTopicInputJsonName(), config.getMessageCount()));
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        executorService.awaitTermination(2, TimeUnit.MINUTES);
        executorService.execute(() -> {
            try {
                String consumerId = "aggregatorByTime";
                Consumer consumer = ConsumerCreator.createConsumer(
                        client, config.getTopicOutputGroupName(), consumerId,
                        String.format("%s_%s", SubscriptionNameEnum.SUBSCRIPTION_KEY_NAME, consumerId),
                        true);
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
