name := "KafkaConsumer"

version := "0.2.0"

scalaVersion := "2.11.8"


val sparkVersion = "2.3.0"

//libraryDependencies += "org.apache.kafka" %% "kafka" % sparkVersion
//libraryDependencies += "org.apache.kafka" % "kafka-clients" % sparkVersion
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming"            % sparkVersion,  //kafka consumer
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,  //kafka consumer
  "com.hortonworks" % "shc-core" % "1.1.1-2.1-s_2.11"                 //hbase writing
)
//libraryDependencies ++= Seq(
//  "org.apache.spark" %% "spark-core" % sparkVersion,
//  "org.apache.spark" %% "spark-streaming" % sparkVersion,
//  "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.0.0"
//)
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.last
}