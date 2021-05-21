package ru.syntez.integration.pulsar.usecases.run;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import ru.syntez.integration.pulsar.config.PulsarConfig;
import ru.syntez.integration.pulsar.entities.DataSizeEnum;
import ru.syntez.integration.pulsar.entities.ResultReport;
import ru.syntez.integration.pulsar.scenarios.ProducerTestScenario;
import ru.syntez.integration.pulsar.scenarios.ProducerWithSizeData;
import ru.syntez.integration.pulsar.usecases.ResultOutputUsecase;
import ru.syntez.integration.pulsar.usecases.StartConsumerUsecase;
import ru.syntez.integration.pulsar.usecases.create.ConsumerCreatorUsecase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.logging.Logger;

/**
 * Запуск обработки для проверки производительности для заданного количества продьюсеров и с заданным объемом сообщения
 *
 * @author Skyhunter
 * @date 19.05.2021
 */
public class RunNonPersistantTestUsecase {

    private final static Logger LOG = Logger.getLogger(RunNonPersistantTestUsecase.class.getName());

    public static void execute(
            PulsarConfig config,
            PulsarClient client,
            int producerCount,
            DataSizeEnum dataSize
    ) throws InterruptedException, ExecutionException {

        LOG.info(String.format("******************** Запуск пересылки сообщений от %s продюсеров с размером сообщений = %s ...", producerCount, dataSize.getDescription()));

        ExecutorService executorService = Executors.newFixedThreadPool(producerCount * 2);

        List<Future<ResultReport>> promiseList = new ArrayList<>();

        String topicName = config.getTopicNonPersistentName();

        for (int index = 0; index < 1; index++) {
            final String consumerId = String.format("consumer_%s", index);
            promiseList.add(executorService.submit(new Callable() {
                public Object call() throws Exception {
                    try {
                        Consumer consumer = ConsumerCreatorUsecase.execute(
                                client, config, topicName, consumerId,
                                String.format("%s_%s", dataSize, consumerId));
                        ResultReport result = StartConsumerUsecase.execute(consumer, config.getRecordLogOutputEnabled(), config.getTimeoutReceive());
                        consumer.close();
                        return result;
                    } catch (PulsarClientException | InterruptedException e) {
                        e.printStackTrace();
                        return null;
                    }
                }
            }));
        }

        for (int index = 0; index < producerCount; index++) {
            final String producerId = String.format("producer_%s", index);
            promiseList.add(executorService.submit(new Callable() {
                public Object call() throws Exception {
                    ProducerTestScenario testScenario = new ProducerWithSizeData(client, config);
                    return testScenario.run(topicName, dataSize, producerId);
                }
            }));
        }

        // ожидание пока executor service не закончит выполнение всех future тасков
        while (true) {
            if (isDone(promiseList)) {
                ResultOutputUsecase.execute(getResult(promiseList), producerCount, config.getTimeoutReceive());
                executorService.shutdown();
                return;
            }
        }
    }

    private static Boolean isDone(List<Future<ResultReport>> promiseList) {
        for (Future<ResultReport> result : promiseList) {
            if (!result.isDone()) {
                return false;
            }
        }
        return true;
    }

    private static List<ResultReport> getResult(List<Future<ResultReport>> promiseList) {
        List<ResultReport> resultList = new ArrayList<>();
        for (Future<ResultReport> result : promiseList) {
            if (result.isDone()) {
                try {
                    resultList.add(result.get());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }
        return resultList;
    }

}
