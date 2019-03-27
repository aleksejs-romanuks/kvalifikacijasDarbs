package com.aleksejs.kafka

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import org.slf4j.LoggerFactory

object KafkaCosumer extends App {
  val logger = LoggerFactory.getLogger(classOf[Nothing].getName)

  val topicName = "test"

  val consumerProps = new Properties()

  consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
  consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
  consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-kafka-consumer")

  val kafkaConsumer = new KafkaConsumer[String, String](consumerProps)
  kafkaConsumer.subscribe(java.util.Collections.singletonList(topicName))

  while (true) {
    val records = kafkaConsumer.poll(Duration.ofMillis(100))

    import scala.collection.JavaConversions._
    for (record <- records) {
      logger.info("Key = " + record.key + " Value = " + record.value)
      logger.info("Partition = " + record.partition + " Offset = " + record.offset)
    }

  }

}
