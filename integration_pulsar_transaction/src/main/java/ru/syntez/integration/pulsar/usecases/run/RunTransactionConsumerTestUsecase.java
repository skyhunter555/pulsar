package ru.syntez.integration.pulsar.usecases.run;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.Transaction;
import ru.syntez.integration.pulsar.config.PulsarConfig;
import ru.syntez.integration.pulsar.entities.SubscriptionNameEnum;
import ru.syntez.integration.pulsar.scenarios.ProducerTestScenario;
import ru.syntez.integration.pulsar.scenarios.ProducerWithTransaction;
import ru.syntez.integration.pulsar.usecases.ResultOutputUsecase;
import ru.syntez.integration.pulsar.usecases.StartConsumerTransactionUsecase;
import ru.syntez.integration.pulsar.usecases.create.ConsumerCreatorUsecase;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Запуск обработки для проверки транзакции при приеме сообщений
 *
 * @author Skyhunter
 * @date 18.05.2021
 */
public class RunTransactionConsumerTestUsecase {

    private static AtomicInteger msgSentCounter = new AtomicInteger(0);
    private static Map recordSetMap = new ConcurrentHashMap<>();

    public static void execute(
            PulsarConfig config,
            PulsarClient client
    ) throws InterruptedException, ExecutionException {

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.execute(() -> {
            ProducerTestScenario testScenario = new ProducerWithTransaction(client, config);
            try {
                Transaction txn = client
                        .newTransaction()
                        .withTransactionTimeout(5, TimeUnit.MINUTES)
                        .build()
                        .get();
                msgSentCounter.set(testScenario.run(config.getTopic1Name(), txn));
                txn.commit().get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        });

        executorService.awaitTermination(10, TimeUnit.SECONDS);

        executorService.execute(() -> {
            try {
                Transaction txn = client
                        .newTransaction()
                        .withTransactionTimeout(5, TimeUnit.MINUTES)
                        .build()
                        .get();
                String consumerId = "consumeAbort";
                Consumer consumer = ConsumerCreatorUsecase.execute(
                        client, config, config.getTopic1Name(), consumerId,
                        String.format("%s_%s", SubscriptionNameEnum.SUBSCRIPTION_KEY_NAME.getCode(), consumerId));
                recordSetMap.put(consumerId, StartConsumerTransactionUsecase.execute(consumer, config.getRecordLogOutputEnabled(), txn));
                txn.abort().get();
                consumer.close();
            } catch (PulsarClientException | InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });
        executorService.execute(() -> {
            try {
                Transaction txn = client
                        .newTransaction()
                        .withTransactionTimeout(5, TimeUnit.MINUTES)
                        .build()
                        .get();
                String consumerId = "consumeCommit";
                Consumer consumer = ConsumerCreatorUsecase.execute(
                        client, config, config.getTopic1Name(), consumerId,
                        String.format("%s_%s", SubscriptionNameEnum.SUBSCRIPTION_KEY_NAME.getCode(), consumerId));
                recordSetMap.put(consumerId, StartConsumerTransactionUsecase.execute(consumer, config.getRecordLogOutputEnabled(), txn));
                txn.commit().get();
                consumer.close();
            } catch (PulsarClientException | InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });
        executorService.awaitTermination(10, TimeUnit.SECONDS);

        ResultOutputUsecase.execute(msgSentCounter.get(), recordSetMap);
    }
}
