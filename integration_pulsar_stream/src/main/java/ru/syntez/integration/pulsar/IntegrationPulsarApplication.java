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

             //кейс Применение преобразования формата сообщения в реальном времени в потоковом режиме.
            LOG.info("Запуск проверки преобразования формата сообщения...");
            runConsumersWithTransform();
            LOG.info("Проверка преобразования формата сообщения завершена.");
            ResultOutput.outputResult(msg_sent_counter.get(), msg_received_counter.get(), consumerRecordSetMap);
            resetResults();

            //кейс Агрегация по 100 сообщений из одного топика с целью получения единого сообщения содержащего данные всех переданных сообщений.
            //Агрегация сообщений из разных топиков.
            LOG.info("Запуск проверки агрегации по 100 сообщений...");
            runConsumersWithAggregationByCount();
            LOG.info("Проверка агрегации по 100 сообщений завершена.");
            ResultOutput.outputResult(msg_sent_counter.get(), msg_received_counter.get(), consumerRecordSetMap);
            resetResults();

            //кейс Событие в формате JSON содержит поле целое числовое поле amount
            //Рассчитать сумму по полю amount за последнюю минуту
            LOG.info("Запуск проверки агрегации за последнюю минуту...");
            runConsumersWithAggregationByTime();
            LOG.info("Проверка агрегации за последнюю минуту завершена.");
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

    /**
     * Запуск обработки сообщений после трансформации
     *
     * @throws InterruptedException
     */
    private static void runConsumersWithTransform() throws InterruptedException {
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

    private static void runConsumersWithAggregationByCount() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        executorService.execute(() -> {
            try {
                runProducerWithKeys(config.getTopicInputGroupName());
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        executorService.execute(() -> {
            try {
                String consumerId = "filter";
                Consumer consumer = ConsumerCreator.createConsumer(
                        client, config.getTopicOutputGroupName(), consumerId,
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

    private static void runConsumersWithAggregationByTime() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        executorService.execute(() -> {
            try {
                runProducerWithKeys(config.getTopicInputGroupName());
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        executorService.execute(() -> {
            try {
                String consumerId = "filter";
                Consumer consumer = ConsumerCreator.createConsumer(
                        client, config.getTopicOutputGroupName(), consumerId,
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

    private static void startConsumer(Consumer<byte[]> consumer) throws PulsarClientException {
        while (msg_received_counter.get() < config.getMessageCount()) {
            Message message = consumer.receive(1, TimeUnit.SECONDS);
            if (message == null) {
                continue;
            }
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
        String messageXml = "<?xml version=\"1.0\" encoding=\"windows-1251\"?>\n" +
                "<OutputDocumentExt>\n" +
                "  <documentId>1</documentId>\n" +
                "  <documentType>order</documentType>\n" +
                "  <documentNumber>123</documentNumber>\n" +
                "</OutputDocumentExt>";
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