name := "KafkaConsumer"

version := "1.1.0"

scalaVersion := "2.11.8"

resolvers += "Hortonworks Repository" at "http://repo.hortonworks.com/content/repositories/releases/"
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

val sparkVersion = "2.3.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion ,
  "org.apache.spark" %% "spark-sql" % sparkVersion ,
  "org.apache.spark" %% "spark-streaming"            % sparkVersion,  //kafka consumer
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,  //kafka consumer
  "com.hortonworks"   % "shc-core" % "1.1.1-2.1-s_2.11",               //hbase writing
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
  "org.scalatest" %% "scalatest" % "3.0.7" % Test,
  "MrPowers" % "spark-fast-tests" % "0.17.1-s_2.11" % Test
)



assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.last
}