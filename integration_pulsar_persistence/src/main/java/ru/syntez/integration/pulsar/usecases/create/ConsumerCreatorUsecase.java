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
     * @param withKeys         - признак наличия ключа в сообщении
     * @param redeliveryEnable - признак передоставки сообщения
     * @return
     * @throws PulsarClientException
     */
    public static Consumer<byte[]> execute(
            PulsarClient pulsarClient,
            PulsarConfig pulsarConfig,
            String topicName,
            String consumerId,
            String subscriptionName,
            Boolean withKeys,
            Boolean redeliveryEnable
    ) throws PulsarClientException {
        SubscriptionType subscriptionType;
        if (withKeys) {
            subscriptionType = SubscriptionType.Key_Shared;
        } else {
            subscriptionType = SubscriptionType.Shared;
        }
        int redeliveryCount;
        int ackTimeout;
        if (redeliveryEnable) {
            redeliveryCount = pulsarConfig.getMaxRedeliveryCount();
            ackTimeout = 1;
        } else {
            redeliveryCount = 0;
            ackTimeout = 0;
        }
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .consumerName(consumerId)
                .topic(topicName)
                .ackTimeout(ackTimeout, TimeUnit.SECONDS)
                .subscriptionType(subscriptionType)
                .subscriptionName(subscriptionName)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .deadLetterPolicy(DeadLetterPolicy.builder()
                        .maxRedeliverCount(redeliveryCount)
                        .deadLetterTopic(pulsarConfig.getTopicDeadLetterName())
                        .build())
                .subscribe();

        LOG.info(String.format("Consumer created: ID=%s; TOPIC=%s; subscriptionType=%s; subscriptionName=%s",
                consumerId, topicName, subscriptionType, subscriptionName));

        return consumer;
    }
}
