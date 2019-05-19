package com.uberpalform.sparkexport

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.expr
import scala.io.Source
import org.apache.spark.sql.functions.col

object SparkExport extends LazyLogging{

  def main(args: Array[String]): Unit = {

    logger.info("Starting reading configuration file ... ")
    val exportConfigString = Source.fromFile(args(0)).getLines.mkString

    logger.info("Starting parsing configurations ... ")
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
       """.stripMargin)


    logger.info("Starting spark session ... ")
    val ss = SparkSession
      .builder
      .appName("SparkExport")
      .getOrCreate()


    logger.info("Creating hbase reader ... ")
    val hbaseReaderObject = new HbaseReader(ss,hbaseConfig.hbaseTableNamespace,hbaseConfig.hbaseTableName,hbaseConfig.hbaseTableColumnFamily)

    logger.info("Getting new requests ... ")
    val inRequests = hbaseReaderObject.readWithRequestCatalog().filter(hbaseConfig.hbaseFilter)

    if (inRequests.count() > 0 ) {

      logger.info(s"Renaming hbase rowkey on ${hbaseConfig.hbaseRowkeyNewInstanceName}")
      val withRenamedColumn = inRequests.withColumnRenamed("rowkey", hbaseConfig.hbaseRowkeyNewInstanceName)

      logger.info(s"Selecting starting table ...")
      val startingDF = ss.table(exportConfig.startingTable._1)
        .join(withRenamedColumn, Seq(hbaseConfig.hbaseRowkeyNewInstanceName), "inner")
        .select(exportConfig.startingTable._2.map(colString => col(colString)): _*)


      val joinedExport = exportConfig.joins.foldLeft(startingDF)((acc: DataFrame, joinCfg) => {
        logger.info(s"Joining ${joinCfg.tableName} on ${joinCfg.condition}")
        acc.join(
            ss.table(joinCfg.tableName),
            expr(joinCfg.condition),
            "left_outer"
          )
          .select(joinCfg.exportColumns
          .map(colString => col(colString)): _*)
      })


      new EmailSender(ss).sendEmails(
        joinedExport,
        hbaseConfig.hbaseRowkeyNewInstanceName,
        hbaseConfig.hbaseTableNamespace,
        hbaseConfig.hbaseTableName,
        hbaseConfig.hbaseTableColumnFamily
      )

    } else {
      logger.warn("No new requests")
    }

  }
}
