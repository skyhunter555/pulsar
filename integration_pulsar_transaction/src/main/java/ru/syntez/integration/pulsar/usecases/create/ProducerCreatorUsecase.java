package ru.syntez.integration.pulsar.usecases.create;

import org.apache.pulsar.client.api.*;

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 *  Создание продюсера для отправки сообщений
 *  @author Skyhunter
 *  @date 03.05.2021
 */
public class ProducerCreatorUsecase {

    private final static Logger LOG = Logger.getLogger(ProducerCreatorUsecase.class.getName());

    /**
     * @param pulsarClient     - ссылка на созданный клиент Pulsar
     * @param topicName        - наименование топика
     * @return
     * @throws PulsarClientException
     */
    public static Producer<byte[]> execute(
            PulsarClient pulsarClient,
            String topicName
    ) throws PulsarClientException {

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .producerName("producer-1")
                .sendTimeout(0, TimeUnit.SECONDS)
                //.compressionType(CompressionType.LZ4)
                .create();

        LOG.info(String.format("Producer created: ID=%s; TOPIC=%s.", producer.getProducerName(), topicName));

        return producer;
    }
}
