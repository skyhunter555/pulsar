## Apache Pulsar: пример поддержки реализации message транзакции

#Конфигурация кластера

Кластер из 3 брокеров
5 bookies
1 топик 6 партиций
1 продюсер
1 консюмер

#Кейсы для реализации 
Транзакционная запись в несколько топиков и чтение из них.
Откат транзакции при сбое в операции.

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

#Создание топиков
pulsar-admin topics create-partitioned-topic \
  persistent://public/namespace-demo/topic-part6-demo \
  --partitions 6

#Настройка транзакционного режима
1. Включить параметр
   broker.conf
   stanalone.conf
   transactionCoordinatorEnabled=true

2. Выполнить команду инициализации координатора транзакций
   bin/pulsar initialize-transaction-coordinator-metadata -cs 127.0.0.1:2181 -c standalone
   
## Build
mvn clean install

## Запуск приложения с конфигурацией
java -jar integration-pulsar-transaction-1.0.0.jar application.yml
