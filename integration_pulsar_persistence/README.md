## Apache Pulsar: пример поддержки реализации отсутствия потерь данных

#Конфигурация кластера

Кластер из 3 брокеров
5 bookies
1 топик 6 партиций
1 продюсер
1 консюмер

#Кейсы для реализации 
Автоматический реконнект клиентов при сбое узла кластера.
Отключение сохранения сообщений.
Запись в режиме по умолчанию без сохранения сообщений
Вычитка сообщений после перезагрузки брокеров.
Сообщения не должны быть потеряны в случае отключения брокера в кластере
Сообщения не должны быть потеряны в случае отключения узла BookKeeper в кластере
Возможность изменения параметров без недоступности
Ранее записанные сообщения могут быть вычитаны вновь подключившимся потребителем

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
pulsar-admin topics create-partitioned-topic \
  persistent://public/namespace-demo/topic-part6-demo \
  --partitions 6
  
## Build
mvn clean install

## Запуск приложения с конфигурацией
java -jar integration-pulsar-persistence-1.0.0.jar application.yml
