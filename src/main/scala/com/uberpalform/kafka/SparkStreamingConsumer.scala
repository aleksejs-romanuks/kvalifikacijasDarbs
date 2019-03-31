package com.uberpalform.kafka;

object SparkStreamingConsumer extends App {

  val conf = new SparkConf().setAppName(SparkStreamingConsumer.getClass.getName)


  val logger = LoggerFactory.getLogger(SparkStreamingConsumer.getClass.getName)

  logger.warn("Initializing spark streaming context ... ")
  val streamingContext = new StreamingContext(conf, Seconds(10))
  streamingContext.sparkContext.setLogLevel("WARN")

  logger.warn("Setting configurations for kafka consumer ... ")
//  val kafkaParams = Map[String, Object](
//    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "sandbox-hdp.hortonworks.com:6667",
//    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
//    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
//    ConsumerConfig.GROUP_ID_CONFIG -> "com.uber.kafka.rightsOfAccess",
//    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
//  )
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "sandbox-hdp.hortonworks.com:6667",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "com.uber.kafka.rightsOfAccess",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val topics = Array("test")


  logger.warn("Creating kafka streaming ... ")
  val kafkaStream = KafkaUtils.createDirectStream[String, String](
    streamingContext,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams)
  )
  kafkaStream.map(record=> record.value.toString).print
  //kafkaMessages.map(record => record.value())
  logger.warn("Staring input data transformations and loadings ... ")
  kafkaStream.foreachRDD( (rdd: RDD[ConsumerRecord[String, String]], time: Time) =>
    if(!rdd.isEmpty){

      rdd.foreach(message =>
        if(!message.value().isEmpty){
          println("Message " + message.value.toString)
          logger.warn(s"Message ${message.value.toString}")
        }else{
          logger.warn(s"Empty value")
        }

      )
    }else{
      logger.warn(s"Empty rdd")
    }
  )


  streamingContext.start()
  streamingContext.awaitTermination()
}
