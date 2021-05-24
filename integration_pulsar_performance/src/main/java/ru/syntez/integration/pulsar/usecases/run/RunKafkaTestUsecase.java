package ru.syntez.integration.pulsar.usecases.run;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import ru.syntez.integration.pulsar.config.PulsarConfig;
import ru.syntez.integration.pulsar.entities.DataSizeEnum;
import ru.syntez.integration.pulsar.entities.ResultReport;
import ru.syntez.integration.pulsar.entities.RoutingDocument;
import ru.syntez.integration.pulsar.usecases.ResultOutputUsecase;
import ru.syntez.integration.pulsar.usecases.StartKafkaConsumerUsecase;
import ru.syntez.integration.pulsar.usecases.kafka.ConsumerCreator;
import ru.syntez.integration.pulsar.usecases.kafka.ProducerCreator;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Запуск обработки kafka для проверки производительности для заданного количества продьюсеров и с заданным объемом сообщения
 *
 * @author Skyhunter
 * @date 20.05.2021
 */
public class RunKafkaTestUsecase {

    private final static Logger LOG = Logger.getLogger(RunKafkaTestUsecase.class.getName());
    private final static String payload1Kb = "This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload repeated for test.This is doc payload.";

    public static void execute(
            PulsarConfig config,
            int producerCount,
            DataSizeEnum dataSize
    ) throws InterruptedException, ExecutionException {

        LOG.info(String.format("******************** Запуск пересылки сообщений от %s продюсеров с размером сообщений = %s ...", producerCount, dataSize.getDescription()));

        ExecutorService executorService = Executors.newFixedThreadPool(producerCount * 2);

        List<Future<ResultReport>> promiseList = new ArrayList<>();

        //for (int index = 0; index < producerCount; index++) {
        //    final String consumerId = String.format("consumer_%s", index);
            promiseList.add(executorService.submit(new Callable() {
                public Object call() throws Exception {
                    try {
                        Consumer consumer = ConsumerCreator.createConsumer(config.getKafka(), String.format("%s_%s", dataSize, "consumerId"));
                        ResultReport result = StartKafkaConsumerUsecase.execute(consumer, config.getRecordLogOutputEnabled(), config.getKafka().getMessageCount() * producerCount);
                        consumer.close();
                        return result;
                    } catch (Exception e) {
                        e.printStackTrace();
                        return null;
                    }
                }
            }));
        //}

        StringBuffer docData = new StringBuffer();
        for (int index = 0; index < dataSize.getFactor(); index++) {
            docData.append(payload1Kb);
        }

        for (int index = 0; index < producerCount; index++) {
            final String producerId = String.format("producer_%s", index);
            promiseList.add(executorService.submit(new Callable() {
                public Object call() throws Exception {
                    Date startDateTime = new Date();
                    Producer<String, RoutingDocument> producer = ProducerCreator.createProducer(config.getKafka());
                    for (int index = 0; index < config.getKafka().getMessageCount(); index++) {
                        RoutingDocument document = new RoutingDocument().createAny(index, docData.toString());
                        sendMessage(producer,
                                new ProducerRecord<>(config.getKafka().getTopicName(), UUID.randomUUID().toString(), document),
                                index,
                                config.getRecordLogOutputEnabled()
                        );
                    }
                    return new ResultReport(producerId, true, startDateTime, new Date(), config.getKafka().getMessageCount(), BigDecimal.ZERO, BigDecimal.ZERO);
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

    private static void sendMessage(
            Producer<String, RoutingDocument> producer,
            ProducerRecord<String, RoutingDocument> record,
            Integer index,
            boolean recordLogOutputEnabled
    ) {
        try {
            RecordMetadata metadata = producer.send(record).get();
            if (recordLogOutputEnabled) {
                LOG.info(String.format("Record %s sent to partition=%s with key=%s, offset=%s", index, metadata.partition(), record.key(), metadata.offset()));
            }
        } catch (ExecutionException | InterruptedException e) {
            LOG.log(Level.WARNING, "Error in sending record", e);
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
