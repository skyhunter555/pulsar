package ru.syntez.integration.pulsar.usecases.run;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import ru.syntez.integration.pulsar.config.PulsarConfig;
import ru.syntez.integration.pulsar.entities.SubscriptionNameEnum;
import ru.syntez.integration.pulsar.scenarios.ProducerTestScenario;
import ru.syntez.integration.pulsar.scenarios.ProducerWithoutInterval;
import ru.syntez.integration.pulsar.usecases.ResultOutputUsecase;
import ru.syntez.integration.pulsar.usecases.StartConsumerUsecase;
import ru.syntez.integration.pulsar.usecases.create.ConsumerCreatorUsecase;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Запуск обработки для проверки сохранения сообщений в кластере при изменении брокеров
 *
 * @author Skyhunter
 * @date 13.05.2021
 */
public class RunClusterBrokerTestUsecase {

    private final static Logger LOG = Logger.getLogger(ConsumerCreatorUsecase.class.getName());

    private static AtomicInteger msgSentCounter = new AtomicInteger(0);
    private static Map recordSetMap = new ConcurrentHashMap<>();

    private static String BROKER_1 = "standalone";
    private static String BROKER_2 = "broker-2";
    private static String BROKER_3 = "broker-3";
    private static String TENANT = "tenant1";
    private static String NAME_SPACE = "ns1";
    private static String TOPIC = "topic1-demo";

    public static void execute(
            PulsarConfig config,
            PulsarClient client,
            PulsarAdmin admin
    ) throws InterruptedException {

        List<String> allowedClusters = Arrays.asList(BROKER_1, BROKER_2, BROKER_3);
        String topicName = String.format("persistent://%s/%s/%s", TENANT, NAME_SPACE, TOPIC);

        //try {
        //    //admin.namespaces().getNamespaces(String.format("%s/%s", TENANT, NAME_SPACE));
        //    admin.tenants().deleteTenant(TENANT);
        //    //admin.clusters().deleteCluster(BROKER_1);
        //    admin.clusters().deleteCluster(BROKER_2);
        //    admin.clusters().deleteCluster(BROKER_3);
        //    LOG.info(String.format("Tenant %s deleted", TENANT));
        //} catch (PulsarAdminException e) {
        //    e.printStackTrace();
        //}

         try {
            createTenant(admin, config, allowedClusters);
         } catch (PulsarAdminException e) {
             e.printStackTrace();
         }

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.execute(() -> {
            ProducerTestScenario testScenario = new ProducerWithoutInterval(client, config);
            msgSentCounter.set(testScenario.run(config.getTopicName()));
        });

        executorService.awaitTermination(20, TimeUnit.SECONDS);

        executorService.execute(() -> {
            try {
                String consumerId = "broker3";
                Consumer consumer = ConsumerCreatorUsecase.execute(
                        client, config, config.getTopicName(), consumerId,
                        String.format("%s_%s", SubscriptionNameEnum.SUBSCRIPTION_KEY_NAME.getCode(), consumerId));
                recordSetMap.put(consumerId, StartConsumerUsecase.execute(consumer, config.getRecordLogOutputEnabled(), 0, false));
                consumer.close();
            } catch (PulsarClientException | InterruptedException e) {
                e.printStackTrace();
            }
        });
        try {
            admin.clusters().deleteCluster(BROKER_3);
            LOG.info(String.format("Broker %s deleted", BROKER_3));
        } catch (PulsarAdminException e) {
            e.printStackTrace();
        }
        executorService.awaitTermination(10, TimeUnit.SECONDS);
        executorService.execute(() -> {
            try {
                String consumerId = "broker2";
                Consumer consumer = ConsumerCreatorUsecase.execute(
                        client, config, config.getTopicName(), consumerId,
                        String.format("%s_%s", SubscriptionNameEnum.SUBSCRIPTION_KEY_NAME.getCode(), consumerId));
                recordSetMap.put(consumerId, StartConsumerUsecase.execute(consumer, config.getRecordLogOutputEnabled(), 0, false));
                consumer.close();
            } catch (PulsarClientException | InterruptedException e) {
                e.printStackTrace();
            }
        });
        try {
            admin.clusters().deleteCluster(BROKER_2);
            LOG.info(String.format("Broker %s deleted", BROKER_2));
        } catch (PulsarAdminException e) {
            e.printStackTrace();
        }
        executorService.awaitTermination(10, TimeUnit.SECONDS);
        executorService.execute(() -> {
            try {
                String consumerId = "broker1";
                Consumer consumer = ConsumerCreatorUsecase.execute(
                        client, config, config.getTopicName(), consumerId,
                        String.format("%s_%s", SubscriptionNameEnum.SUBSCRIPTION_KEY_NAME.getCode(), consumerId));
                recordSetMap.put(consumerId, StartConsumerUsecase.execute(consumer, config.getRecordLogOutputEnabled(), 0, false));
                consumer.close();
            } catch (PulsarClientException | InterruptedException e) {
                e.printStackTrace();
            }
        });
        try {
            admin.tenants().deleteTenant(TENANT);
            LOG.info(String.format("Tenant %s deleted", TENANT));
        } catch (PulsarAdminException e) {
            e.printStackTrace();
        }
        executorService.awaitTermination(10, TimeUnit.SECONDS);

        ResultOutputUsecase.execute(msgSentCounter.get(), recordSetMap);
    }

    private static void createTenant(PulsarAdmin admin, PulsarConfig config, List<String> allowedClusters) throws PulsarAdminException {
        //ClusterData broker1 = createBroker(admin, BROKER_1, String.format("%s:%s", config.getBrokersUrl(), config.getBrokerPort1()));
        ClusterData broker2 = createBroker(admin, BROKER_2, String.format("%s:%s", config.getBrokersUrl(), config.getBrokerPort2()));
        ClusterData broker3 = createBroker(admin, BROKER_3, String.format("%s:%s", config.getBrokersUrl(), config.getBrokerPort2()));
        admin.tenants().createTenant(TENANT, new TenantInfo(ImmutableSet.of("admin"), Sets.newHashSet(allowedClusters)));
        admin.namespaces().createNamespace(String.format("%s/%s", TENANT, NAME_SPACE), Sets.newHashSet(allowedClusters));
         //admin.topics().createPartitionedTopic(topicName,6);
    }

    private static ClusterData createBroker(PulsarAdmin admin, String clusterName, String clusterBrokerUrl) throws PulsarAdminException {
        admin.clusters().createCluster(clusterName, new ClusterData(null, null));
        ClusterData cluster = admin.clusters().getCluster(clusterName);
        cluster.setBrokerServiceUrl(clusterBrokerUrl);
        return cluster;
    }

}
