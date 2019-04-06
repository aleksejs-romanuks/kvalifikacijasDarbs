package com.uberpalform.kafka

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions.col

object SparkStreamingConsumer extends App {

  val conf = new SparkConf().setAppName(SparkStreamingConsumer.getClass.getName)

  val logger = LoggerFactory.getLogger(SparkStreamingConsumer.getClass.getName)

  logger.warn("Initializing spark streaming context ... ")
  val streamingContext = new StreamingContext(conf, Seconds(10))
  streamingContext.sparkContext.setLogLevel("WARN")

  logger.warn("Initializing spark streaming session ... ")
  val ss = SparkSession
    .builder
    .appName("SparkStreamingConsumer")
    .getOrCreate()


  logger.warn("Setting configurations for kafka consumer ... ")
  val kafkaParams = Map[String, Object](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG         -> "sandbox-hdp.hortonworks.com:6667",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG    -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG  -> classOf[StringDeserializer],
    ConsumerConfig.GROUP_ID_CONFIG                  -> "com.uber.kafka.rightsOfAccess",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG         -> "latest",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG        -> (false: java.lang.Boolean)
  )

  val topics = Array("test")

  logger.warn("Creating kafka streaming ... ")
  val kafkaStream = KafkaUtils.createDirectStream[String, String](
    streamingContext,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams)
  )
//  val jsonRequests = kafkaStream.map(record=> record.value.toString)

  logger.warn("Creating request parsing object ... ")
  val jsonRequestParserObject = new JsonRequestParser(ss)
//  val requests = jsonRequests.map(jsonRequestParserObject.parseJson(_))


  logger.warn("Creating hbase writing object ... ")
  val hbaseWriterObject = new HbaseWriter("test", "test", "test")



  logger.warn("Staring input data transformations and loadings ... ")
  kafkaStream.foreachRDD( (rdd: RDD[ConsumerRecord[String, String]]) =>
    if(!rdd.isEmpty){
      val pureMessages = rdd.filter(_.value() != null).map(_.value())
      val parsedDF = jsonRequestParserObject.parseJsonRDDtoDF(pureMessages)
      parsedDF.show()
      val parsedDfWithKeyColumn = parsedDF.withColumn("rowkey", col("customerId"))
      parsedDfWithKeyColumn.show()
      hbaseWriterObject.writeRequestHbase(parsedDfWithKeyColumn)
    }else{
      logger.warn(s"Empty rdd")
    }
  )
  streamingContext.start()
  streamingContext.awaitTermination()
}
