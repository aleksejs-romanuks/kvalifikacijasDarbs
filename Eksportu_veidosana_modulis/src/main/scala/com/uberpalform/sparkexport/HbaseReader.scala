package com.uberpalform.sparkexport

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.{DataFrame, SparkSession}

/***
  * HbaseReader class is designed to read requests from hbase table
  * @param namespace - hbase table namespace
  * @param table - hbase table name
  * @param columnFamily - hbase tables column family
  */

class HbaseReader (spark : SparkSession, namespace : String, table : String, columnFamily : String) extends LazyLogging {
  private val requestCatalog =
    s"""{
       |"table":{"namespace":"$namespace", "name":"$table"},
       |"rowkey":"key",
       |"columns":{
       |"rowkey":{"cf":"rowkey", "col":"key", "type":"string"},
       |"customerId":{"cf":"$columnFamily", "col":"customerId", "type":"string"},
       |"role":{"cf":"$columnFamily", "col":"role", "type":"string"},
       |"status":{"cf":"$columnFamily", "col":"status", "type":"string"},
       |"requestDateTime":{"cf":"$columnFamily", "col":"requestDateTime", "type":"string"}
       |}
       |}""".stripMargin

  logger.info(s"hbase table mapping: $requestCatalog")

  /***
    * readWithRequestCatalog function is performing reading from hbase table
    */
  def readWithRequestCatalog() : DataFrame = {
    logger.info(s"Starting reading from hbase table $table")
    spark.sqlContext.read
      .options(Map(HBaseTableCatalog.tableCatalog->requestCatalog))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
  }
}
