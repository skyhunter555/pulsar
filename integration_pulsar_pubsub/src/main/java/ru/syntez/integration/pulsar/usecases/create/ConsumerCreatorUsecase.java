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
     * @param readCompacted    - признак сжатия топика
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
            Boolean readCompacted,
            Boolean redeliveryEnable
    ) throws PulsarClientException {

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
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .readCompacted(readCompacted)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionName(subscriptionName + "_" + consumerId)
                .deadLetterPolicy(DeadLetterPolicy.builder()
                        .maxRedeliverCount(redeliveryCount)
                        .deadLetterTopic(pulsarConfig.getTopicDeadLetterName())
                        .build())
                .subscribe();

        LOG.info(String.format("Consumer created: ID=%s; TOPIC=%s; subscriptionType=%s; subscriptionName=%s",
                consumerId, topicName, consumer.getSubscription(), subscriptionName));

        return consumer;
    }
}
