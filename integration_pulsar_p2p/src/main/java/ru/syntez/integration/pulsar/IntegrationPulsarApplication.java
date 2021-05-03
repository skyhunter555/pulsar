package ru.syntez.integration.pulsar;

import com.fasterxml.jackson.core.JsonProcessingException;
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

    private static final String SUBSCRIPTION_NAME = "shared-demo";
    private static final String SUBSCRIPTION_KEY_NAME = "key-shared-demo";

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
            runConsumersWithoutTimeout(3, false, false, config.getTopicAtleastName());
            LOG.info("Проверка гарантии доставки ATLEAST_ONCE завершена.");
            ResultOutput.outputResult(msg_sent_counter.get(), msg_received_counter.get(), consumerRecordSetMap);
            resetResults();

            //кейс ATMOST_ONCE
            LOG.info("Запуск проверки гарантии доставки ATMOST_ONCE...");
            runConsumersWithoutTimeout(3, true, false, config.getTopicAtmostName());
            LOG.info("Проверка гарантии доставки ATMOST_ONCE завершена.");
            ResultOutput.outputResult(msg_sent_counter.get(), msg_received_counter.get(), consumerRecordSetMap);
            resetResults();

            //кейс EFFECTIVELY_ONCE
            // TODO сделать отдельное пространство имен с топиком
            LOG.info("Запуск проверки гарантии доставки EFFECTIVELY_ONCE + дедупликация...");
            runConsumersWithoutTimeout(3, true, true, config.getTopicEffectivelyName());
            LOG.info("Проверка гарантии доставки EFFECTIVELY_ONCE + дедупликация завершена.");
            ResultOutput.outputResult(msg_sent_counter.get(), msg_received_counter.get(), consumerRecordSetMap);
            resetResults();

            //кейс TTL
            LOG.info("Запуск проверки TTL...");
            runConsumersWithTimeout(3, config.getTopicName());
            LOG.info("Проверка TTL завершена.");
            ResultOutput.outputResult(msg_sent_counter.get(), msg_received_counter.get(), consumerRecordSetMap);
            resetResults();

            //кейс Routing
            LOG.info(String.format("Запуск проверки Routing...", msg_sent_counter.get()));
            runConsumersWithRouting();
            LOG.info("Проверка Routing завершена.");
            ResultOutput.outputResult(msg_sent_counter.get(), msg_received_counter.get(), consumerRecordSetMap);
            resetResults();

            //кейс Filter
            LOG.info(String.format("Запуск проверки Filter...", msg_sent_counter.get()));
            runConsumersWithFilter();
            LOG.info("Проверка Filter завершена.");
            ResultOutput.outputResult(msg_sent_counter.get(), msg_received_counter.get(), consumerRecordSetMap);
            resetResults();

            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        LOG.info("Проверка всех кейсов завершена.");

    }

    private static void resetResults() {
        msg_sent_counter = new AtomicInteger(0);
        msg_received_counter = new AtomicInteger(0);
        consumerRecordSetMap = new ConcurrentHashMap<>();
    }

    private static void runConsumersWithFilter() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        executorService.execute(() -> {
            try {
                runProducerWithKeys(config.getTopicInputFilterName());
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        executorService.execute(() -> {
            try {
                String consumerId = "filter";
                Consumer consumer = ConsumerCreator.createConsumer(
                        client, config.getTopicOutputFilterName(), consumerId,
                        String.format("%s_%s", SUBSCRIPTION_KEY_NAME, consumerId),
                        true);
                startConsumer(consumer);
                consumer.close();
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }
        });
        executorService.shutdown();
        //Минуты должно хватить на обработку всех сообщений
        executorService.awaitTermination(1, TimeUnit.MINUTES);
    }

    /**
     * Запуск обработки сообщений после маршрутизации
     *
     * @throws InterruptedException
     */
    private static void runConsumersWithRouting() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        executorService.execute(() -> {
            try {
                runProducerWithKeys(config.getTopicInputRouteName());
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        executorService.execute(() -> {
            try {
                String consumerId = "order";
                Consumer consumer = ConsumerCreator.createConsumer(
                        client, config.getTopicOutputOrderName(), consumerId,
                        String.format("%s_%s", SUBSCRIPTION_KEY_NAME, consumerId),
                        true
                );
                startConsumer(consumer);
               // consumer.close();
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }
        });
        executorService.execute(() -> {
            try {
                String consumerId = "invoice";
                Consumer consumer = ConsumerCreator.createConsumer(
                        client, config.getTopicOutputInvoiceName(), consumerId,
                        String.format("%s_%s", SUBSCRIPTION_KEY_NAME, consumerId),
                        true
                );
                startConsumer(consumer);
               // consumer.close();
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }
        });
        executorService.shutdown();
        //Минуты должно хватить на обработку всех сообщений
        executorService.awaitTermination(1, TimeUnit.MINUTES);
    }

    /**
     * Запуск одновременной обработки
     *
     * @param consumerCount - количество консьюмеров
     * @param withKeys      - флаг генерации ключей сообщений
     * @param withVersions  - флаг генерации нескольких сообщений для одного ключа
     * @param  topicName    - наименование топика
     * @throws InterruptedException
     */
    private static void runConsumersWithoutTimeout(Integer consumerCount, Boolean withKeys, Boolean withVersions, String topicName) throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(consumerCount + 1);
        if (withKeys) {
            if (withVersions) {
                executorService.execute(() -> {
                    runProducerWithVersions(topicName);
                });
            } else {
                executorService.execute(() -> {
                    try {
                        runProducerWithKeys(topicName);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }
        } else {
            executorService.execute(() -> {
                runProducerWithoutKeys(topicName);
            });
        }
        for (int i = 0; i < consumerCount; i++) {
            String consumerId = Integer.toString(i + 1);
            executorService.execute(() -> {
                Consumer consumer = null;
                try {
                    consumer = createAndStartConsumer(consumerId, withKeys, topicName);
                } catch (PulsarClientException e) {
                    e.printStackTrace();
                } finally {
                    if (consumer != null) {
                        try {
                            consumer.close();
                        } catch (PulsarClientException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }
        executorService.shutdown();
        //Минуты должно хватить на обработку всех сообщений
        executorService.awaitTermination(1, TimeUnit.MINUTES);
    }

    /**
     * Запуск обработки через время после записи
     *
     * @param consumerCount
     * @throws InterruptedException
     */
    private static void runConsumersWithTimeout(Integer consumerCount, String topicName) throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(consumerCount + 1);
        executorService.execute(() -> {
            try {
                runProducerWithKeys(topicName);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        executorService.awaitTermination(config.getTimeoutBeforeConsume(), TimeUnit.MINUTES);

        for (int i = 0; i < consumerCount; i++) {
            String consumerId = Integer.toString(i + 1);
            executorService.execute(() -> {
                Consumer consumer = null;
                try {
                    consumer = createAndStartConsumer(consumerId, true, topicName);
                } catch (PulsarClientException e) {
                    e.printStackTrace();
                } finally {
                    if (consumer != null) {
                        try {
                            consumer.close();
                        } catch (PulsarClientException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }
        executorService.shutdown();
        //Минуты должно хватить на обработку всех сообщений
        executorService.awaitTermination(1, TimeUnit.MINUTES);

    }

    /**
     * Отправка сообщений без ключа для первого кейса - проверка гарантии at-least-once
     * TODO выделить в отдельный юзкейс
     */
    private static void runProducerWithoutKeys(String topicName) {
        RoutingDocument document = loadDocument();
        try {
            Producer<byte[]> producer = client.newProducer()
                    .topic(topicName)
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
     * TODO выделить в отдельный юзкейс
     */
    private static void runProducerWithKeys(String topicName) throws PulsarClientException, JsonProcessingException {
        RoutingDocument document = loadDocument();
        Producer<byte[]> producer = client.newProducer()
                .topic(topicName)
                .compressionType(CompressionType.LZ4)
                .create();
        for (int index = 0; index < config.getMessageCount(); index++) {
            document.setDocId(index);
            if ((index % 2) == 0) {
                document.setDocType(DocumentTypeEnum.order);
            } else {
                document.setDocType(DocumentTypeEnum.invoice);
            }
            byte[] msgValue = xmlMapper().writeValueAsString(document).getBytes();
            String messageKey = document.getDocType().name() + "_" + getMessageKey(index, KeyTypeEnum.UUID);
            producer.newMessage()
                    .key(messageKey)
                    .value(msgValue)
                    .property("my-key", "my-value")
                    .send();
            msg_sent_counter.incrementAndGet();
            //LOG.info("Send message " + index + "; Key=" + messageKey + "; topic = " + topicName);
        }
        producer.flush();
        LOG.info(String.format("Количество отправленных уникальных сообщений: %s", msg_sent_counter.get()));

    }

    /**
     * Генерация сообщений с тремя версиями на один ключ
     * TODO выделить в отдельный юзкейс
     */
    private static void runProducerWithVersions(String topicName) {
        RoutingDocument document = loadDocument();
        try {
            Producer<byte[]> producer = client.newProducer()
                    .topic(topicName)
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

    private static Consumer createAndStartConsumer(String consumerId, Boolean withKeys, String topicName) throws PulsarClientException {
        String subscriptionName;
        if (withKeys) {
            subscriptionName = SUBSCRIPTION_KEY_NAME;
        } else {
            subscriptionName = SUBSCRIPTION_NAME;
        }
        Consumer<byte[]> consumer = ConsumerCreator.createConsumer(
                client,
                topicName,
                consumerId,
                subscriptionName,
                withKeys);
        startConsumer(consumer);
        return consumer;
    }

    private static void startConsumer(Consumer<byte[]> consumer) throws PulsarClientException {
        while (true) {
            Message message = consumer.receive(5, TimeUnit.SECONDS);
            if (message == null) {
                LOG.info("No message to consume after waiting for 5 seconds.");
                break;
            }
            consumer.acknowledge(message);
            msg_received_counter.incrementAndGet();

            Set consumerRecordSet = consumerRecordSetMap.get(consumer.getConsumerName());
            if (consumerRecordSet == null) {
                consumerRecordSet = new HashSet();
            }
            consumerRecordSet.add(message.getValue());
            consumerRecordSetMap.put(consumer.getConsumerName(), consumerRecordSet);

            //LOG.info(String.format("Consumer %s read record key=%s, number=%s, value=%s, topic=%s",
            //        consumerId,
            //        message.getKey(),
            //        msg_received_counter,
            //        new String(message.getData()),
            //        message.getTopicName()
            //));
        }
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
