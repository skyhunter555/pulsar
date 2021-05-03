package ru.syntez.integration.pulsar;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.JacksonXmlModule;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.apache.pulsar.client.api.*;
import org.yaml.snakeyaml.Yaml;
import ru.syntez.integration.pulsar.entities.DocumentTypeEnum;
import ru.syntez.integration.pulsar.entities.KeyTypeEnum;
import ru.syntez.integration.pulsar.entities.RoutingDocument;
import ru.syntez.integration.pulsar.exceptions.TestMessageException;
import ru.syntez.integration.pulsar.pulsar.ConsumerCreator;
import ru.syntez.integration.pulsar.utils.ResultOutput;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Main class
 *
 * @author Skyhunter
 * @date 23.04.2021
 */
public class IntegrationPulsarApplication {

    private final static Logger LOG = Logger.getLogger(ru.syntez.integration.pulsar.IntegrationPulsarApplication.class.getName());
    private static AtomicInteger msg_sent_counter = new AtomicInteger(0);
    private static AtomicInteger msg_received_counter = new AtomicInteger(0);
    private static ru.syntez.integration.pulsar.pulsar.PulsarConfig config;
    private static Map<String, Set<String>> consumerRecordSetMap = new ConcurrentHashMap<>();

    private static PulsarClient client;

    private static ObjectMapper xmlMapper() {
        JacksonXmlModule xmlModule = new JacksonXmlModule();
        xmlModule.setDefaultUseWrapper(false);
        ObjectMapper xmlMapper = new XmlMapper(xmlModule);
        xmlMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        return new XmlMapper(xmlModule);
    }

    public static void main(String[] args) {

        Yaml yaml = new Yaml();
        //try( InputStream in = Files.newInputStream( Paths.get( args[ 0 ] ) ) ) {
        try (InputStream in = Files.newInputStream(Paths.get(IntegrationPulsarApplication.class.getResource("/application.yml").toURI()))) {
            config = yaml.loadAs(in, ru.syntez.integration.pulsar.pulsar.PulsarConfig.class);
            LOG.log(Level.INFO, config.toString());
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Error load PulsarConfig from resource", e);
            return;
        }

        try {
            client = PulsarClient.builder()
                    .serviceUrl(config.getBrokers())
                    .build();

            //кейс ATLEAST_ONCE
            LOG.info("Запуск проверки гарантии доставки ATLEAST_ONCE...");
            runConsumersWithoutTimeout(3, false, false);
            LOG.info("Проверка гарантии доставки ATLEAST_ONCE завершена.");
            ResultOutput.outputResult(msg_sent_counter.get(), msg_received_counter.get(), consumerRecordSetMap);
            resetResults();

            //кейс ATMOST_ONCE
            LOG.info("Запуск проверки гарантии доставки ATMOST_ONCE...");
            runConsumersWithoutTimeout(3, true, false);
            LOG.info("Проверка гарантии доставки ATMOST_ONCE завершена.");
            ResultOutput.outputResult(msg_sent_counter.get(), msg_received_counter.get(), consumerRecordSetMap);
            resetResults();

            //кейс EFFECTIVELY_ONCE
            LOG.info("Запуск проверки гарантии доставки EFFECTIVELY_ONCE + дедупликация...");
            runConsumersWithoutTimeout(3, true, true);
            LOG.info("Проверка гарантии доставки EFFECTIVELY_ONCE + дедупликация завершена.");
            ResultOutput.outputResult(msg_sent_counter.get(), msg_received_counter.get(), consumerRecordSetMap);
            resetResults();

            //кейс TTL
            LOG.info("Запуск проверки TTL...");
            runConsumersWithTimeout(3);
            LOG.info("Проверка TTL завершена.");
            ResultOutput.outputResult(msg_sent_counter.get(), msg_received_counter.get(), consumerRecordSetMap);
            resetResults();
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        //Result

    }

    private static void resetResults() {
        msg_sent_counter = new AtomicInteger(0);
        msg_received_counter = new AtomicInteger(0);
        consumerRecordSetMap = new ConcurrentHashMap<>();
    }

    private static void runConsumersWithoutTimeout(Integer consumerCount, Boolean withKeys, Boolean withVersions) throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(consumerCount + 1);
        if (withKeys) {
            if (withVersions) {
                executorService.execute(ru.syntez.integration.pulsar.IntegrationPulsarApplication::runProducerWithVersions);
            } else {
                executorService.execute(ru.syntez.integration.pulsar.IntegrationPulsarApplication::runProducerWithKeys);
            }
        } else {
            executorService.execute(ru.syntez.integration.pulsar.IntegrationPulsarApplication::runProducerWithoutKeys);
        }
        for (int i = 0; i < consumerCount; i++) {
            String consumerId = Integer.toString(i + 1);
            executorService.execute(() -> {
                try {
                    startConsumer(consumerId);
                } catch (PulsarClientException e) {
                    e.printStackTrace();
                }
            });
        }
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);
    }

    private static void runConsumersWithTimeout(Integer consumerCount) throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(consumerCount + 1);
        executorService.execute(ru.syntez.integration.pulsar.IntegrationPulsarApplication::runProducerWithKeys);

        executorService.awaitTermination(config.getTimeoutBeforeConsume(), TimeUnit.MINUTES);

        for (int i = 0; i < consumerCount; i++) {
            String consumerId = Integer.toString(i + 1);
            executorService.execute(() -> {
                try {
                    startConsumer(consumerId);
                } catch (PulsarClientException e) {
                    e.printStackTrace();
                }
            });
        }
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);

    }

    /**
     * Отправка сообщений без ключа для первого кейса - проверка гарантии at-least-once
     */
    private static void runProducerWithoutKeys() {
        RoutingDocument document = loadDocument();
        try {
            Producer<byte[]> producer = client.newProducer()
                    .topic(config.getTopicName())
                    .compressionType(CompressionType.LZ4)
                    .create();
            for (int index = 0; index < config.getMessageCount(); index++) {
                document.setDocId(index);
                document.setDocType(DocumentTypeEnum.invoice);
                byte[] msgValue = xmlMapper().writeValueAsString(document).getBytes();
                producer.newMessage().value(msgValue).send();
                msg_sent_counter.incrementAndGet();
                //LOG.info("Send message " + index);
            }
            producer.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Отправка сообщений с уникальным ключом для второго кейса - проверка гарантии at-most-once
     */
    private static void runProducerWithKeys() {
        RoutingDocument document = loadDocument();
        try {
            Producer<byte[]> producer = client.newProducer()
                    .topic(config.getTopicName())
                    .compressionType(CompressionType.LZ4)
                    .create();
            for (int index = 0; index < config.getMessageCount(); index++) {
                document.setDocId(index);
                document.setDocType(DocumentTypeEnum.invoice);
                byte[] msgValue = xmlMapper().writeValueAsString(document).getBytes();
                String messageKey = getMessageKey(index, KeyTypeEnum.UUID);
                producer.newMessage()
                        .key(messageKey)
                        .value(msgValue)
                        .property("my-key", "my-value")
                        .send();
                msg_sent_counter.incrementAndGet();
                //LOG.info("Send message " + index + "; Key=" + messageKey);
            }
            producer.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Генерация сообщений с тремя версиями на один ключ
     */
    private static void runProducerWithVersions() {
        RoutingDocument document = loadDocument();
        try {
            Producer<byte[]> producer = client.newProducer()
                    .topic(config.getTopicName())
                    .compressionType(CompressionType.LZ4)
                    .create();
            Map<Integer, String> keyMap = new HashMap<>();
            for (int index = 0; index < config.getMessageCount(); index++) {
                document.setDocId(index);
                document.setDocType(DocumentTypeEnum.unknown);
                String keyValue = getMessageKey(index, KeyTypeEnum.NUMERIC);
                keyMap.put(index, keyValue);
                byte[] msgValue = xmlMapper().writeValueAsString(document).getBytes();
                producer.newMessage().key(keyValue).value(msgValue).send();
                msg_sent_counter.incrementAndGet();
            }

            for (int index = 0; index < config.getMessageCount(); index++) {
                document.setDocId(index);
                document.setDocType(DocumentTypeEnum.order);
                byte[] msgValue = xmlMapper().writeValueAsString(document).getBytes();
                producer.newMessage().key(keyMap.get(index)).value(msgValue).send();
                msg_sent_counter.incrementAndGet();
            }

            for (int index = 0; index < config.getMessageCount(); index++) {
                document.setDocId(index);
                document.setDocType(DocumentTypeEnum.invoice);
                byte[] msgValue = xmlMapper().writeValueAsString(document).getBytes();
                producer.newMessage().key(keyMap.get(index)).value(msgValue).send();
                msg_sent_counter.incrementAndGet();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static void startConsumer(String consumerId) throws PulsarClientException {

        Consumer<byte[]> consumer = ConsumerCreator.createConsumer(client, config.getTopicName(), consumerId);

        while (true) {
            Message message = consumer.receive(5, TimeUnit.SECONDS);
            if (message == null) {
                LOG.info("No message to consume after waiting for 5 seconds.");
                break;
            }
            consumer.acknowledge(message);
            msg_received_counter.incrementAndGet();

            Set consumerRecordSet = consumerRecordSetMap.get(consumerId);
            if (consumerRecordSet == null) {
                consumerRecordSet = new HashSet();
            }
            consumerRecordSet.add(message.getValue());
            consumerRecordSetMap.put(consumerId, consumerRecordSet);

            //LOG.info(String.format("Consumer %s read record key=%s, number=%s, value=%s, topic=%s",
            //        consumerId,
            //        message.getKey(),
            //        msg_received_counter,
            //        new String(message.getData()),
            //        message.getTopicName()
            //));
        }
        consumer.close();
    }

    private static String getMessageKey(Integer index, KeyTypeEnum keyType) {
        if (KeyTypeEnum.ONE.equals(keyType)) {
            return "key_1";
        } else if (KeyTypeEnum.UUID.equals(keyType)) {
            return UUID.randomUUID().toString();
        } else if (KeyTypeEnum.NUMERIC.equals(keyType)) {
            return String.format("key_%s", index);
        } else {
            return null;
        }
    }

    private static RoutingDocument loadDocument() {
        String messageXml = "<?xml version=\"1.0\" encoding=\"windows-1251\"?><routingDocument><docId>1</docId><docType>order</docType></routingDocument>";
        RoutingDocument document;
        try {
            document = xmlMapper().readValue(messageXml, RoutingDocument.class);
        } catch (IOException e) {
            LOG.log(Level.WARNING, "Error readValue from resource", e);
            throw new TestMessageException(e);
        }
        return document;
    }

}
