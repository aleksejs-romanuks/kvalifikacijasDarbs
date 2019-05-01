name := "sparkexport"

version := "0.1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"      % sparkVersion,
  "org.apache.spark" %% "spark-sql"       % sparkVersion,
  "com.hortonworks"   % "shc-core" % "1.1.1-2.1-s_2.11",               //hbase reader
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0"
)