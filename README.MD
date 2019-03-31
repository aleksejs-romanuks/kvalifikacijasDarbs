<b>Kafka location:</b>
cd /usr/hdp/current/kafka-broker/bin/

<b>Start kafka console consumer:</b>
/kafka-console-consumer.sh --bootstrap-server sandbox-hdp.hortonworks.com:6667 --topic test --from-beginning

<b>Start kafka console producer:</b>
/usr/hdp/current/kafka-broker/bin/kafka-console-producer.sh --broker-list sandbox-hdp.hortonworks.com:6667 --topic test

<b>Send build on cluster via scp:</b>
scp -P 2222 KafkaConsumer-assembly-0.1.1.jar maria_dev@sandbox-hdp.hortonworks.com:/home/maria_dev/kafka_project

<b>Ssh on cluster:</b>
ssh -p 2222 maria_dev@127.0.0.1

<b>Access hortonworks web UI:</b>
http://127.0.0.1:8080/#/login