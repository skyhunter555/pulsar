package ru.syntez.integration.pulsar.usecases;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.Transaction;
import ru.syntez.integration.pulsar.entities.ResultReport;
import ru.syntez.integration.pulsar.entities.RoutingDocument;
import ru.syntez.integration.pulsar.exceptions.TestMessageException;

import java.io.IOException;
import java.util.Date;
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
 * @date 19.05.2021
 */
public class StartConsumerUsecase {

    private final static Logger LOG = Logger.getLogger(StartConsumerUsecase.class.getName());

    /**
     * @param consumer               - созданный экземпляр обработчика
     * @param recordLogOutputEnabled - флаг вывода каждого обработанного сообщения в лог
     * @return
     * @throws PulsarClientException
     */
    public static ResultReport execute(
            Consumer<byte[]> consumer,
            boolean recordLogOutputEnabled,
            int timeoutReceiveSeconds
    ) throws PulsarClientException, InterruptedException {

        AtomicInteger msgReceivedCounter = new AtomicInteger(0);
        Date startDateTime = new Date();
        while (true) {
            Message message = consumer.receive(timeoutReceiveSeconds, TimeUnit.SECONDS);
            if (message == null) {
               // LOG.info(String.format("No message to consume after waiting for %s seconds.", timeoutReceiveSeconds));
                break;
            }
            consumer.acknowledge(message.getMessageId());
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
        return new ResultReport(consumer.getConsumerName(), false, startDateTime, new Date(), msgReceivedCounter.get());
    }

}
