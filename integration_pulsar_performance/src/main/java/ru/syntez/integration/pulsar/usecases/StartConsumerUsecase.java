package ru.syntez.integration.pulsar.usecases;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.Transaction;
import ru.syntez.integration.pulsar.entities.ResultReport;
import ru.syntez.integration.pulsar.entities.RoutingDocument;
import ru.syntez.integration.pulsar.exceptions.TestMessageException;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
            int timeoutReceiveSeconds,
            boolean calculateLatency
    ) throws PulsarClientException, InterruptedException {

        AtomicInteger msgReceivedCounter = new AtomicInteger(0);
        AtomicLong publishLatencySum = new AtomicLong(0);
        AtomicLong endToEndLatencySum = new AtomicLong(0);
        Date startDateTime = new Date();
        while (true) {
            Message message = consumer.receive(timeoutReceiveSeconds, TimeUnit.SECONDS);
            if (message == null) {
               // LOG.info(String.format("No message to consume after waiting for %s seconds.", timeoutReceiveSeconds));
                break;
            }
            long publishLatency = 0;
            long endToEndLatency = 0;
            //Для вычисление Latency необходимо распарсить документ, что увеличивает общее время обработки
            if (calculateLatency) {
                long receiveTime = new Date().getTime();
                try {
                    RoutingDocument document = DeserializeDocumentUsecase.execute(message.getData());
                    publishLatency = message.getPublishTime() - document.getDocTime();
                    endToEndLatency = receiveTime - document.getDocTime();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            consumer.acknowledge(message.getMessageId());
            msgReceivedCounter.incrementAndGet();
            publishLatencySum.set(publishLatencySum.get() + publishLatency);
            endToEndLatencySum.set(endToEndLatencySum.get() + endToEndLatency);
            if (recordLogOutputEnabled) {
                LOG.info(
                        String.format("Consumer %s read record key=%s, number=%s, messageId=%s, publishLatency=%s, endToEndLatency=%s, topic=%s",
                        consumer.getConsumerName(),
                        message.getKey(),
                        msgReceivedCounter,
                        message.getMessageId(),
                        publishLatency,
                        endToEndLatency,
                        message.getTopicName()
                ));
            }
        }
        return new ResultReport(
                consumer.getConsumerName(),
                false,
                startDateTime, new Date(),
                msgReceivedCounter.get(),
                BigDecimal.valueOf(publishLatencySum.get()).divide(BigDecimal.valueOf(msgReceivedCounter.get()), 2, RoundingMode.HALF_UP),
                BigDecimal.valueOf(endToEndLatencySum.get()).divide(BigDecimal.valueOf(msgReceivedCounter.get()),2, RoundingMode.HALF_UP)
        );
    }

}
