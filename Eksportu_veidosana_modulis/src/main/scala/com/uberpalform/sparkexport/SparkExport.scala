package com.uberpalform.sparkexport

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.exp
import scala.io.Source

object SparkExport extends LazyLogging{
  def main(args: Array[String]): Unit = {
    val exportConfigString = Source.fromFile(args(0)).getLines.mkString

    val configFileParserObject = new ConfigFileParser(exportConfigString)
    val hbaseConfig = configFileParserObject.getHbaseTableInfo
    val exportConfig = configFileParserObject.getExportConfig

    logger.info(
      s"""
        |Printing hbase info config:
        |   table name = ${hbaseConfig.hbaseTableName}
        |   namespace = ${hbaseConfig.hbaseTableNamespace}
        |   column family = ${hbaseConfig.hbaseTableColumnFamily}
        |   columns = ${hbaseConfig.hbaseColumns.mkString(", ")}
        |   filter = ${hbaseConfig.hbaseFilter}
      """.stripMargin)

    logger.info(
      s"""
         |Printing export config:
         |    starting hive table = ${exportConfig.startingTable._1}
         |    starting columns = ${exportConfig.startingTable._2}
         |    email column = ${exportConfig.emailColumn}
         |    joins = ${exportConfig.joins.mkString(",")}
         |    output path = ${exportConfig.outputPath}
       """.stripMargin)

    val ss = SparkSession
      .builder
      .appName("SparkExport")
      .getOrCreate()

    val hbaseReaderObject = new HbaseReader(ss,hbaseConfig.hbaseTableNamespace,hbaseConfig.hbaseTableName,hbaseConfig.hbaseTableColumnFamily)

    val inRequests = hbaseReaderObject.readWithRequestCatalog().filter(hbaseConfig.hbaseFilter)

    inRequests.show()

    val joinedExport = exportConfig.joins.foldLeft(inRequests)((acc : DataFrame, joinCfg) => {
      logger.info(s"Joining ${joinCfg.tableName} on ${joinCfg.commonColumn}")
      acc.join(ss.table(joinCfg.tableName), Seq(joinCfg.commonColumn), "inner").select(joinCfg.exportColumns)
    })

    joinedExport.show()


  }
}
