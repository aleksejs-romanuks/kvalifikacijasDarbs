package com.uberpalform.kafka

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.hbase._

object HbaseWriter extends App {
  val CONFIG_HBASE = "/etc/hbase/conf/hbase-site.xml"

  val ss = SparkSession
    .builder
    .appName("ROAExportUniversal")
    .getOrCreate()

  import ss.implicits._
  val someDF = Seq(
    ("1", "aleksejs", "romanuks", "21"),
    ("2", "aleksejs", "romanuks", "21"),
    ("3", "aleksejs", "romanuks", "21")
  ).toDF("rowkey", "name", "surname", "age")
  val catalog = s"""{
                   	|"table":{"namespace":"test", "name":"test"},
                   	|"rowkey":"key",
                   	|"columns":{
                   		|"rowkey":{"cf":"rowkey", "col":"key", "type":"string"},
                   		|"name":{"cf":"test", "col":"name", "type":"string"},
                   		|"surname":{"cf":"test", "col":"surname", "type":"string"},
                   		|"age":{"cf":"test", "col":"age", "type":"string"}
                   	|}
                   |}""".stripMargin
  someDF.show()
  println(catalog)
//  val df = ss.sqlContext
//    .read
//    .options(Map(HBaseTableCatalog.tableCatalog->catalog.toString))
//    .format("org.apache.spark.sql.execution.datasources.hbase")
//    .load()
  someDF.write.options(
    Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
    .format("org.apache.spark.sql.execution.datasources.hbase")
    .save()
}
