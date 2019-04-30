package com.uberpalform.kafka

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.DataFrame

/***
  * HbaseWriter class is designed to write requests into the hbase table
  * @param namespace - hbase table namespace
  * @param table - hbase table name
  * @param columnFamily - hbase tables column family
  */

class HbaseWriter (namespace : String, table : String, columnFamily : String) extends LazyLogging{
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
    * writeRequestHbase function writes dataframe into hbase table
    * @param inDf - dataframe to write into the habse table. Dataframe schema should be ([rowkey, StringType], [customerId, StringType],[requestDateTime, StringType], [status, StringType])
    */

  def writeRequestHbase(inDf :DataFrame): Unit = {
    if(inDf.count() > 0) {
      inDf.write.options(
        Map(HBaseTableCatalog.tableCatalog -> requestCatalog, HBaseTableCatalog.newTable -> "5"))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()
    }else logger.warn("No request to insert. ")
  }
}
