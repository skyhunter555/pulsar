## Apache Pulsar: пример поддержки реализации очереди сообщений Pub-Sub

#Конфигурация кластера

Кластер из 3 брокеров и 6 узлов bookies
Топик с  6 партициями
1 продюсер
3 консюмера в одной группе

#Кейсы для реализации 
1. Гарантия доставки ATLEAST_ONCE - сообщение будет записано в партицию топика хотя бы один раз.
Сообщения отправляются одним продюсером в топик topic-part6-demo и одновременно вычитываются тремя косьюмерами с помощью EXCLUSIVE подписки.
Количество отправленных уникальных сообщений должно быть не меньше количества принятых сообщений.
Ключ сообщений не задан. 

2. Гарантия доставки ATMOST_ONCE - сообщение будет записано в партицию топика не более одного раза.
Сообщения отправляются одним продюсером в топик topic-part6-demo и одновременно вычитываются тремя косьюмерами с помощью EXCLUSIVE подписки.
Для сообщений указывается уникальный ключ.

Настройка дедупликация для пространства имен:
sudo ./pulsar-admin namespaces set-deduplication public/namespace-demo --enable false

3. Гарантия доставки EFFECTIVELY_ONCE + дедупликация - ровно-однократная

a) Сообщения отправляются одним продюсером в топик "persistent://public/namespace-demo/topic-effectively-demo" 
    и одновременно вычитываются тремя косьюмерами с помощью EXCLUSIVE подписки.
    Для сообщений указывается уникальный ключ. На один ключ отправляется три версии сообщения.
    Количество отправленных уникальных сообщений должно быть равно количеству принятых сообщений.

б) Сообщения отправляются одним продюсером в топик "persistent://public/namespace-demo/topic-effectively-compacted-demo" 
   и одновременно вычитываются тремя косьюмерами с помощью EXCLUSIVE подписки.
   Для сообщений указывается уникальный ключ. На один ключ отправляется три версии сообщения.
   Количество отправленных уникальных сообщений должно быть в три раза больше количества принятых сообщений.
   
Настройка автоматического запуска уплотнения:
sudo ./pulsar-admin namespaces set-compaction-threshold --threshold 1M public/namespace-demo

Запуск уплотнения вручную:
sudo ./pulsar-admin topics compact persistent://public/namespace-demo/topic-effectively-compacted-demo

Настройка дедупликация для пространства имен:
sudo ./pulsar-admin namespaces set-deduplication public/namespace-demo --enable true

4. Поддержка времени жизни сообщений TTL без настройки retention
Сообщения отправляются одним продюсером в топик topic-part6-demo без вычитывания.
Спустя 3 минуты сообщения вычитываются тремя косьюмерами с помощью SHARED подписки.
После превышения времени 2 минуты сообщения помечаются как удаленные и не должны быть вычитаны.

Настройка TTL для пространства имен:
sudo ./pulsar-admin namespaces set-message-ttl public/namespace-demo --messageTTL 120 

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
