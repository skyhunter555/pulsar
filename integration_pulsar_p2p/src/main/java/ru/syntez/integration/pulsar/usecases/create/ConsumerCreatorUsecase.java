package ru.syntez.integration.pulsar.usecases.create;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
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
     * @return
     * @throws PulsarClientException
     */
    public static Consumer<byte[]> execute(
            PulsarClient pulsarClient,
            String topicName,
            String consumerId,
            String subscriptionName,
            Boolean withKeys
    ) throws PulsarClientException {
        SubscriptionType subscriptionType;
        if (withKeys) {
            subscriptionType = SubscriptionType.Key_Shared;
        } else {
            subscriptionType = SubscriptionType.Shared;
        }
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .consumerName(consumerId)
                .topic(topicName)
                .subscriptionType(subscriptionType)
                .subscriptionName(subscriptionName)
                .subscribe();

        LOG.info(String.format("Consumer created: ID=%s; TOPIC=%s; subscriptionType=%s; subscriptionName=%s",
                consumerId, topicName, subscriptionType, subscriptionName));

        return consumer;
    }
}
