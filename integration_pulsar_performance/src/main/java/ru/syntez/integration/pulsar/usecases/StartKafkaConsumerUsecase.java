package ru.syntez.integration.pulsar.usecases;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import ru.syntez.integration.pulsar.entities.ResultReport;
import ru.syntez.integration.pulsar.usecases.kafka.ConsumerCreator;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Запуск консюмера на обработку сообщений из заданной подписки
 *
 * @author Skyhunter
 * @date 20.05.2021
 */
public class StartKafkaConsumerUsecase {

    private final static Logger LOG = Logger.getLogger(StartKafkaConsumerUsecase.class.getName());

    /**
     * @param consumer               - созданный экземпляр обработчика
     * @param recordLogOutputEnabled - флаг вывода каждого обработанного сообщения в лог
     * @return
     * @throws PulsarClientException
     */
    public static ResultReport execute(
            org.apache.kafka.clients.consumer.Consumer consumer,
            boolean recordLogOutputEnabled,
            int messageCount
    ) throws PulsarClientException, InterruptedException {

        AtomicInteger msgReceivedCounter = new AtomicInteger(0);
        Date startDateTime = new Date();
        while (msgReceivedCounter.get() < messageCount) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<String, String> record : records) {
                //record.timestamp();
                msgReceivedCounter.incrementAndGet();
                if (recordLogOutputEnabled) {
                    LOG.info(String.format("Consumer read record key=%s, number=%s, value=%s, partition=%s, offset = %s, ",
                            record.key(),
                            msgReceivedCounter.get(),
                            record.value(),
                            record.partition(),
                            record.offset()
                    ));
                }
            }
            consumer.commitSync();
        }
        return new ResultReport("kafkaConsumer", false, startDateTime, new Date(), msgReceivedCounter.get(), BigDecimal.ZERO, BigDecimal.ZERO);
    }

}
