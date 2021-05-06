package ru.syntez.integration.pulsar.usecases;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import ru.syntez.integration.pulsar.entities.RoutingDocument;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 *  Запуск консюмера на обработку сообщений из заданной подписки
 *  В результате получается коллекция уникальных сообщений, сгруппированная по идентификаторам консьюмеров, для вывода результатов
 *  Логирование каждой записи опционально
 *
 *  @author Skyhunter
 *  @date 05.05.2021
 */
public class StartConsumerUsecase {

    private final static Logger LOG = Logger.getLogger(StartConsumerUsecase.class.getName());

    /**
     * @param consumer                  - созданный экземпляр обработчика
     * @param recordLogOutputEnabled    - флаг вывода каждого обработанного сообщения в лог
     * @param errorDocIds               - коллекция идентификаторов документов для эмуляции ошибок
     * @return
     * @throws PulsarClientException
     */
    public static Set<String> execute(
            Consumer<byte[]> consumer,
            boolean recordLogOutputEnabled,
            Set errorDocIds
    ) throws PulsarClientException {

        Set<String> consumerRecordSet = new HashSet<>();
        AtomicInteger msgReceivedCounter = new AtomicInteger(0);
        Map<String, Message> deadMessageMap = new ConcurrentHashMap<>();

        while (true) {
            Message message = consumer.receive(10, TimeUnit.SECONDS);
            if (message == null) {
                LOG.info("No message to consume after waiting for 10 seconds.");
                break;
            }

            RoutingDocument routingDocument;
            try {
                routingDocument = DeserializeDocumentUsecase.execute(message.getData());
            } catch (IOException e) {
                e.printStackTrace();
                continue;
            }

            //Эмуляция сбоя при обработке сообщения
            if (errorDocIds != null
                    && routingDocument!=null
                    && errorDocIds.contains(routingDocument.getDocId())
                    && deadMessageMap.get(message.getMessageId().toString()) == null
            ) {
                deadMessageMap.put(message.getMessageId().toString(), message);
                LOG.info(String.format("Emulate error on consumer with messageId=%s", message.getMessageId()));
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
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
        }
        return consumerRecordSet;
    }

}
