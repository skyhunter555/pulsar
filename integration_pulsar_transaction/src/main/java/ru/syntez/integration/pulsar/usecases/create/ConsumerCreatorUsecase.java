package ru.syntez.integration.pulsar.usecases.create;

import org.apache.pulsar.client.api.*;
import ru.syntez.integration.pulsar.config.PulsarConfig;

import java.util.concurrent.TimeUnit;
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
            PulsarConfig pulsarConfig,
            String topicName,
            String consumerId,
            String subscriptionName
    ) throws PulsarClientException {

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .consumerName(consumerId)
                .topic(topicName)
                .subscriptionType(SubscriptionType.Exclusive)
                .ackTimeout(10, TimeUnit.SECONDS)
                .subscriptionName(subscriptionName)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        LOG.info(String.format("Consumer created: ID=%s; TOPIC=%s; subscriptionName=%s",
                consumerId, topicName, subscriptionName));

        return consumer;
    }
}
