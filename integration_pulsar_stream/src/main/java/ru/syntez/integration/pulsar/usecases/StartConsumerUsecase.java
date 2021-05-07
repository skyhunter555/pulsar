package ru.syntez.integration.pulsar.usecases;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Запуск консюмера на обработку сообщений из заданной подписки
 * В результате получается коллекция уникальных сообщений, для вывода результатов
 * Логирование каждой записи опционально
 *
 * @author Skyhunter
 * @date 05.05.2021
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
            boolean recordLogOutputEnabled
    ) throws PulsarClientException {

        Set<String> consumerRecordSet = new HashSet<>();
        AtomicInteger msgReceivedCounter = new AtomicInteger(0);

        while (true) {
            Message message = consumer.receive(20, TimeUnit.SECONDS);
            if (message == null) {
                LOG.info("No message to consume after waiting for 20 seconds.");
                break;
            }

            consumer.acknowledge(message);
            consumerRecordSet.add(new String(message.getData()));
            msgReceivedCounter.incrementAndGet();
            if (recordLogOutputEnabled) {
                LOG.info(String.format("Consumer %s read record key=%s, number=%s, messageId=%s, value=%s, topic=%s",
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
