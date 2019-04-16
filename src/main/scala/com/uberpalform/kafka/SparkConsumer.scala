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
import org.apache.spark.sql.functions.{col,lit}

object SparkConsumer extends App {
  case class MissingArgumentException(argumnet : String) extends Exception("Please set argument [" + argumnet + "]") {}

  var topic = ""
  var hbase_table_name = ""
  var hbase_table_namespace = ""
  var hbase_columns_family = ""

  args.sliding(2, 1).toList.collect {
    case Array("--kafka_topic",           arg1: String) => topic = arg1
    case Array("--hbase_table_name",      arg2: String) => hbase_table_name = arg2
    case Array("--hbase_table_namespace", arg3: String) => hbase_table_namespace = arg3
    case Array("--hbase_columns_family",  arg4: String) => hbase_columns_family = arg4
  }
  if (topic.isEmpty)                  throw MissingArgumentException("--kafka_topic")
  if (hbase_table_name.isEmpty)       throw MissingArgumentException("--hbase_table_name")
  if (hbase_table_namespace.isEmpty)  throw MissingArgumentException("--hbase_table_namespace")
  if (hbase_columns_family.isEmpty)   throw MissingArgumentException("--hbase_columns_family")

  val conf = new SparkConf().setAppName("SparkConsumer")

  val logger = LoggerFactory.getLogger("SparkConsumer")

  logger.info("Initializing spark streaming context ... ")
  val streamingContext = new StreamingContext(conf, Seconds(10))
//  streamingContext.sparkContext.setLogLevel("WARN")

  logger.info("Initializing spark streaming session ... ")
  val ss = SparkSession
    .builder
    .appName("SparkConsumer")
    .getOrCreate()


  logger.info("Setting configurations for kafka consumer ... ")
  val kafkaParams = Map[String, Object](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG         -> "sandbox-hdp.hortonworks.com:6667",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG    -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG  -> classOf[StringDeserializer],
    ConsumerConfig.GROUP_ID_CONFIG                  -> "com.uber.kafka.rightsOfAccess",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG         -> "latest",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG        -> (false: java.lang.Boolean)
  )

  val topics = Array(topic)

  logger.info("Creating kafka streaming ... ")
  val kafkaStream = KafkaUtils.createDirectStream[String, String](
    streamingContext,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams)
  )


  logger.info("Creating request parsing object ... ")
  val jsonRequestParserObject = new JsonRequestParser(ss)



  logger.info("Creating hbase writing object ... ")
  val hbaseWriterObject = new HbaseWriter(hbase_table_namespace, hbase_table_name, hbase_columns_family)


  logger.info("Staring input data transformations and loadings ... ")
  kafkaStream.foreachRDD( (rdd: RDD[ConsumerRecord[String, String]]) =>
    if(!rdd.isEmpty){
      logger.info("Filter input messages on nulls ... ")
      val pureMessages = rdd.filter(_.value() != null).map(_.value())

      logger.info("Parsing input rdd of jsons into dataframe ... ")
      val parsedDF = jsonRequestParserObject.parseJsonRDDtoDF(pureMessages)

      val parsedDfWithKeyColumn = parsedDF
        .withColumn("rowkey", col("customerId"))
        .withColumn("status", lit("NEW"))


      logger.info(s"Writing request into hbase table $hbase_table_name ... ")
      hbaseWriterObject.writeRequestHbase(parsedDfWithKeyColumn)
    }else{
      logger.warn(s"Empty rdd")
    }
  )
  streamingContext.start()
  streamingContext.awaitTermination()
}
