package ru.syntez.integration.pulsar.usecases.create;

import org.apache.pulsar.client.api.*;

import java.util.logging.Logger;

/**
 *  Создание консьюмера для приема сообщений
 *  @author Skyhunter
 *  @date 03.05.2021
 */
public class ConsumerCreatorUsecase {

    private final static Logger LOG = Logger.getLogger(ConsumerCreatorUsecase.class.getName());

    /**
     * @param pulsarClient     - ссылка на созданный клиент Pulsar
     * @param topicName        - наименование топика
     * @param consumerId       - идентификатор консьюмера
     * @param subscriptionName - наименование подписки
     * @return
     * @throws PulsarClientException
     */
    public static Consumer<byte[]> execute(
            PulsarClient pulsarClient,
            String topicName,
            String consumerId,
            String subscriptionName,
            Boolean readCompacted
    ) throws PulsarClientException {
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .consumerName(consumerId)
                .topic(topicName)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .readCompacted(readCompacted)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionName(subscriptionName + "_" + consumerId)
                .subscribe();

        LOG.info(String.format("Consumer created: ID=%s; TOPIC=%s; subscriptionType=%s; subscriptionName=%s",
                consumerId, topicName, consumer.getSubscription(), subscriptionName));

        return consumer;
    }
}
