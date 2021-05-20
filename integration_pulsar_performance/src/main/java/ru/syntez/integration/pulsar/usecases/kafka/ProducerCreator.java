package ru.syntez.integration.pulsar.usecases.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.syntez.integration.pulsar.entities.RoutingDocument;

import java.util.Properties;

public class ProducerCreator {

    public static Producer<String, RoutingDocument> createProducer(KafkaConfig kafkaConfig) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBrokers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, RoutingDocumentSerializer.class.getName());

        //props.put(ProducerConfig.CLIENT_ID_CONFIG, kafkaConfig.getProducer().getClientId());
        props.put(ProducerConfig.ACKS_CONFIG, kafkaConfig.getProducer().getAcks());
        props.put(ProducerConfig.RETRIES_CONFIG, kafkaConfig.getProducer().getRetries());
        props.put(ProducerConfig.LINGER_MS_CONFIG, kafkaConfig.getProducer().getLingerMs());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, kafkaConfig.getProducer().getRequestTimeoutMs());
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, kafkaConfig.getProducer().getDeliveryTimeoutMs());

        //props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());

        //Для второго кейса
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        return new KafkaProducer<>(props);
    }

}
