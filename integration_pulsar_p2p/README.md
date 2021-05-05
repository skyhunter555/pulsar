## Apache Pulsar: пример поддержки реализации очереди сообщений Р2Р

#Конфигурация кластера

Кластер из 3 брокеров и 6 узлов bookies
Топик с  6 партициями
1 продюсер
3 консюмера в одной группе

#Кейсы для реализации 
1. Гарантия доставки ATLEAST_ONCE - сообщение будет записано в партицию топика хотя бы один раз.
Сообщения отправляются одним продюсером в топик topic-part6-demo и одновременно вычитываются тремя косьюмерами с помощью SHARED подписки.
Количество отправленных уникальных сообщений должно быть не меньше количества принятых сообщений.
Ключ сообщений не задан. 

2. Гарантия доставки ATMOST_ONCE - сообщение будет записано в партицию топика не более одного раза.
Сообщения отправляются одним продюсером в топик topic-part6-demo и одновременно вычитываются тремя косьюмерами с помощью KEY_SHARED подписки.
Для сообщений указывается уникальный ключ. Количество отправленных уникальных сообщений должно быть не меньше количества принятых сообщений.

Настройка дедупликация для пространства имен:
sudo ./pulsar-admin namespaces set-deduplication public/namespace-demo --disable

3. Гарантия доставки EFFECTIVELY_ONCE + дедупликация - ровно-однократная
Сообщения отправляются одним продюсером в топик topic-part6-demo и одновременно вычитываются тремя косьюмерами с помощью KEY_SHARED подписки.
Для сообщений указывается уникальный ключ. На один ключ отправляется три версии сообщения.
Количество отправленных уникальных сообщений должно быть не меньше количества принятых сообщений.

Настройка дедупликация для пространства имен:
sudo ./pulsar-admin namespaces set-deduplication public/namespace-demo --enable

4. Поддержка времени жизни сообщений TTL без настройки retention
Сообщения отправляются одним продюсером в топик topic-part6-demo без вычитывания.
Спустя 3 минуты сообщения вычитываются тремя косьюмерами с помощью SHARED подписки.
После превышения времени 2 минуты сообщения помечаются как удаленные и не должны быть вычитаны.
При этом необходимо настроить частоту проверки у брокера:
messageExpiryCheckIntervalInMinutes = 60 сек

Настройка TTL для пространства имен:
sudo ./pulsar-admin namespaces set-message-ttl public/namespace-demo --messageTTL 120 

5. Поддержка маршрутизации и фильтрации сообщений
Сообщения отправляются одним продюсером в топик topic-input-demo и в зависимости от значения ключа маршрутизируются в два других топика:
topic-output-order-demo и topic-output-invoice-demo.
Для этого кейса не обязательно создавать отдельно пространство. После загрузки функций топики создаются автоматически.
namespaces = public/default

    а) Маршрутизация сообщений по ключу.
       RoutingByKeyDemoFunction
    б) Фильтрация сообщений по ключу.
       FilterByKeyDemoFunction
    в) Фильтрация сообщений в зависимости от содержания.
       FilterByBodyDemoFunction

#Создание функции:
Для загрузки функци необходимо пометить jar примера в папку apache-pulsar-2.7.1/lib
и выполнить комманду загрузки для каждой функции

sudo ./pulsar-admin functions create \
--jar /opt/apache-pulsar-2.7.1/lib/integration-pulsar-p2p-1.0.0.jar \
--classname ru.syntez.integration.pulsar.functions.RoutingByKeyDemoFunction \ 
--tenant public \ 
--namespace default \
--name routingByKey \
--inputs persistent://public/default/topic-input-route-demo

sudo ./pulsar-admin functions create \
--jar /opt/apache-pulsar-2.7.1/lib/integration-pulsar-p2p-1.0.0.jar \
--classname ru.syntez.integration.pulsar.functions.FilterByKeyDemoFunction \
--tenant public \ 
--namespace default \
--name filterByKey \
--inputs persistent://public/default/topic-input-filter-demo

sudo ./pulsar-admin functions create \
--jar /opt/apache-pulsar-2.7.1/lib/integration-pulsar-p2p-1.0.0.jar \
--classname ru.syntez.integration.pulsar.functions.RoutingByBodyDemoFunction \
--tenant public \ 
--namespace default \
--name routingByBody \
--inputs persistent://public/default/topic-input-route-demo

Удаление функции:
sudo ./pulsar-admin functions delete \
--tenant public \
--namespace default \
--name routingByKey  

# Установка и настройка Apache Pulsar для linux:
https://pulsar.apache.org/docs/en/2.4.0/standalone/

sudo mkdir /opt/pulsar
sudo wget https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=pulsar/pulsar-2.7.1/apache-pulsar-2.7.1-bin.tar.gz
sudo tar xvf apache-pulsar-2.7.1-bin.tar.gz
sudo mv apache-pulsar-2.7.1/ /opt/pulsar
cd /opt/pulsar/bin
sudo ./pulsar standalone

#Создание кластера из 3-х брокеров
sudo ./pulsar-admin clusters create cluster-1 --url http://localhost:8081 --broker-url pulsar://localhost:6651
sudo ./pulsar-admin clusters create cluster-2 --url http://localhost:8082 --broker-url pulsar://localhost:6652
sudo ./pulsar-admin clusters create cluster-3 --url http://localhost:8083 --broker-url pulsar://localhost:6653

#Создание пространства имен
pulsar-admin namespaces create public/namespace-demo

#Настройка TTL у пространства имен

#Создание топиков
1 Для первых 4 кейсов
pulsar-admin topics create-partitioned-topic \
  persistent://public/namespace-demo/topic-part6-demo \
  --partitions 6
  
## Build
mvn clean install

## Запуск приложения с конфигурацией
java -jar integration-pulsar-p2p-1.0.0.jar application.yml
