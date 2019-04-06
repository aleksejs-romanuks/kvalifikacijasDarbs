package com.uberpalform.kafka

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.DataFrame

class HbaseWriter (namespace : String, table : String, columnFamily : String) {
  val requestCatalog = s"""{
                   	|"table":{"namespace":"test", "name":"test"},
                   	|"rowkey":"key",
                      |"columns":{
                        |"rowkey":{"cf":"rowkey", "col":"key", "type":"string"},
                        |"customerId":{"cf":"test", "col":"customerId", "type":"string"},
                        |"requestDateTime":{"cf":"test", "col":"requestDateTime", "type":"string"},
                        |"status":{"cf":"test", "col":"status", "type":"string"}
                     |}
                   |}""".stripMargin

  def writeRequestHbase(inDf :DataFrame)= {
    inDf.write.options(
      Map(HBaseTableCatalog.tableCatalog -> requestCatalog, HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }
}
