package ru.syntez.integration.pulsar.pulsar;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;

import java.util.logging.Logger;

public class ConsumerCreator {

    private final static Logger LOG = Logger.getLogger(ru.syntez.integration.pulsar.IntegrationPulsarApplication.class.getName());

    public static Consumer<byte[]> createConsumer(
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
