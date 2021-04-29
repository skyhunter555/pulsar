package ru.syntez.integration.pulsar.pulsar;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;

public class ConsumerCreator {

    private static final String SUBSCRIPTION_NAME = "shared-demo";
    private static final String SUBSCRIPTION_KEY_NAME = "key-shared-demo";

    public static Consumer<byte[]> createConsumer(PulsarClient pulsarClient, String topicName, String consumerId, Boolean withKeys) throws PulsarClientException {

        SubscriptionType subscriptionType;
        String subscriptionName;
        if (withKeys) {
            subscriptionType = SubscriptionType.Key_Shared;
            subscriptionName = SUBSCRIPTION_KEY_NAME;
        } else {
            subscriptionType = SubscriptionType.Shared;
            subscriptionName = SUBSCRIPTION_NAME;
        }
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .consumerName(consumerId)
                .topic(topicName)
                .subscriptionType(subscriptionType)
                .subscriptionName(subscriptionName)
                .subscribe();
        return consumer;
    }

}
