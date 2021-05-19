package ru.syntez.integration.pulsar.usecases;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.Transaction;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Запуск консюмера на обработку сообщений из заданной подписки
 *
 * @author Skyhunter
 * @date 18.05.2021
 */
public class StartConsumerTransactionUsecase {

    private final static Logger LOG = Logger.getLogger(StartConsumerTransactionUsecase.class.getName());

    /**
     * @param consumer               - созданный экземпляр обработчика
     * @param recordLogOutputEnabled - флаг вывода каждого обработанного сообщения в лог
     * @return
     * @throws PulsarClientException
     */
    public static Set<String> execute(
            Consumer<byte[]> consumer,
            boolean recordLogOutputEnabled,
            Transaction txn
    ) throws PulsarClientException, InterruptedException {

        Set<String> consumerRecordSet = new HashSet<>();
        AtomicInteger msgReceivedCounter = new AtomicInteger(0);

        while (true) {
            Message message = consumer.receive(5, TimeUnit.SECONDS);
            if (message == null) {
                LOG.info("No message to consume after waiting for 5 seconds.");
                break;
            }

            consumer.acknowledgeAsync(message.getMessageId(), txn);

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
