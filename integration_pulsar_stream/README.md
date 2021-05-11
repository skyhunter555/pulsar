## Apache Pulsar: пример поддержки реализации потоковой обработки данных и агрегации

#Конфигурация кластера

Кластер из 3 брокеров и 6 узлов bookies
1 топик для входящих сообщений и два топика для преобразованных исходящих сообщений.
1 продюсер для первого кейса и 2 для второго
3 консюмера в одной группе

#Кейсы для реализации 
1. Применение преобразования формата сообщения в реальном времени в потоковом режиме. 
В топик для входящих сообщений записываются сообщения разного типа. В зависимости от типа сообщения, 
они трансформируются в другой формат и записываются в соответсвующие топики.

2. Агрегирование событий из разных источников данных по кличеству накопленных сообщений.
Агрегация по 100 сообщений из одного топика с целью получения единого сообщения содержащего данные всех переданных сообщений.
Агрегация сообщений из разных топиков.

3. Получение суммы по полю группы событий.
Событие в формате JSON содержит поле целое числовое поле amount.
Рассчитать сумму по полю amount за последнюю минуту.

#Создание функции:
Для загрузки функци необходимо пометить jar примера в папку apache-pulsar-2.7.1/lib
и выполнить комманду загрузки для каждой функции

sudo ./pulsar-admin functions create --jar /opt/pulsar/apache-pulsar-2.7.1/lib/integration-pulsar-stream-1.0.0.jar --classname ru.syntez.integration.pulsar.functions.TransformDemoFunction --tenant public --namespace default --name transformDemo --inputs persistent://public/default/topic-input-demo

sudo ./pulsar-admin functions create --jar /opt/pulsar/apache-pulsar-2.7.1/lib/integration-pulsar-stream-1.0.0.jar --classname ru.syntez.integration.pulsar.functions.AggregationByCountDemoFunction --tenant public --namespace default --name aggrByCountDemo --inputs persistent://public/default/topic-input-invoice-demo,persistent://public/default/topic-input-order-demo

sudo ./pulsar-admin functions create --jar /opt/pulsar/apache-pulsar-2.7.1/lib/integration-pulsar-stream-1.0.0.jar --classname ru.syntez.integration.pulsar.functions.AggregationByTimeDemoFunction --tenant public --namespace default --name aggrByTimeDemo --inputs persistent://public/default/topic-input-json-demo --output persistent://public/default/topic-output-demo --window-length-duration-ms 60000 --sliding-interval-duration-ms 60000

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
cd /opt/pulsar/apache-pulsar-2.7.1/bin
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
java -jar integration-pulsar-stream-1.0.0.jar application.yml
