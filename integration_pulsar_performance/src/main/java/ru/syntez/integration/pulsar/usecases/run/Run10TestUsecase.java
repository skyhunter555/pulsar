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
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Запуск обработки для проверки производительности 10 продьюсеров и консьюмеров
 *
 * @author Skyhunter
 * @date 19.05.2021
 */
public class Run10TestUsecase {

    // private static Map msgSentCounter = new ConcurrentHashMap<>();
    // private static Map recordSetMap = new ConcurrentHashMap<>();

    public static void execute(
            PulsarConfig config,
            PulsarClient client
    ) throws InterruptedException, ExecutionException {

        ExecutorService executorService = Executors.newFixedThreadPool(20);

        List<Future<ResultReport>> promiseList = new ArrayList<>();

        for (int index = 0; index < 10; index++) {
            final String consumerId = String.format("consumer_%s", index);
            promiseList.add(executorService.submit(new Callable() {
                public Object call() throws Exception {
                    try {
                        Consumer consumer = ConsumerCreatorUsecase.execute(
                                client, config, config.getTopicName(), consumerId,
                                String.format("%s_%s", DataSizeEnum.SIZE_1_KB, consumerId));
                        Date startDateTime = new Date();
                        Integer resultCount = StartConsumerUsecase.execute(consumer, config.getRecordLogOutputEnabled());
                        Date endDateTime = new Date();
                        //recordSetMap.put(consumerId, StartConsumerUsecase.execute(consumer, config.getRecordLogOutputEnabled()));
                        consumer.close();
                        return new ResultReport(consumerId, false, startDateTime, endDateTime, resultCount);
                    } catch (PulsarClientException | InterruptedException e) {
                        e.printStackTrace();
                        return null;
                    }
                }
            }));
        }

        for (int index = 0; index < 10; index++) {
            final String producerId = String.format("producer_%s", index);
            promiseList.add(executorService.submit(new Callable() {
                public Object call() throws Exception {
                    ProducerTestScenario testScenario = new ProducerWithSizeData(client, config);
                    Date startDateTime = new Date();
                    Integer resultCount = testScenario.run(config.getTopicName(), DataSizeEnum.SIZE_1_KB, producerId);
                    Date endDateTime = new Date();
                    return new ResultReport(producerId, false, startDateTime, endDateTime, resultCount);
                    //msgSentCounter.put(producerId, testScenario.run(config.getTopicName(), DataSizeEnum.SIZE_1_KB, producerId));
                }
            }));

            // executorService.execute(() -> {
            //     ProducerTestScenario testScenario = new ProducerWithSizeData(client, config);
            //     msgSentCounter.put(producerId, testScenario.run(config.getTopicName(), DataSizeEnum.SIZE_1_KB, producerId));
            // });
        }

        // ожидание пока executor service не закончит выполнение всех future тасков
        while (true) {
            if (isDone(promiseList)) {
                //System.out.println("Done");
                ResultOutputUsecase.execute(getResult(promiseList));
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
