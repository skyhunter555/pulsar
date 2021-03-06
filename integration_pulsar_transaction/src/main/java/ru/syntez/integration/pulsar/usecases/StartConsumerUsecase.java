package ru.syntez.integration.pulsar.usecases;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.Transaction;
import ru.syntez.integration.pulsar.entities.RoutingDocument;
import ru.syntez.integration.pulsar.exceptions.TestMessageException;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Запуск консюмера на обработку сообщений из заданной подписки
 *
 * @author Skyhunter
 * @date 12.05.2021
 */
public class StartConsumerUsecase {

    private final static Logger LOG = Logger.getLogger(StartConsumerUsecase.class.getName());

    /**
     * @param consumer               - созданный экземпляр обработчика
     * @param recordLogOutputEnabled - флаг вывода каждого обработанного сообщения в лог
     * @return
     * @throws PulsarClientException
     */
    public static Set<String> execute(
            Consumer<byte[]> consumer,
            boolean recordLogOutputEnabled,
            int receiveIntervalMs //,
           // Transaction txn
    ) throws PulsarClientException, InterruptedException {

        Set<String> consumerRecordSet = new HashSet<>();
        AtomicInteger msgReceivedCounter = new AtomicInteger(0);

        while (true) {
            Message message = consumer.receive(5, TimeUnit.SECONDS);
            if (message == null) {
                LOG.info("No message to consume after waiting for 5 seconds.");
                break;
            }

            //Эмуляция обработки сообщения
            Thread.sleep(receiveIntervalMs);
            //consumer.acknowledge(message);
            consumer.acknowledgeAsync(message.getMessageId());

            consumerRecordSet.add(new String(message.getData()));
            msgReceivedCounter.incrementAndGet();
            if (recordLogOutputEnabled) {
                LOG.info(
                        String.format("Consumer %s read record key=%s, number=%s, messageId=%s, value=%s, topic=%s",
                        consumer.getConsumerName(),
                        message.getKey(),
                        msgReceivedCounter,
                        message.getMessageId(),
                        new String(message.getData()),
                        message.getTopicName()
                ));
            }
        }
        return consumerRecordSet;
    }

}
