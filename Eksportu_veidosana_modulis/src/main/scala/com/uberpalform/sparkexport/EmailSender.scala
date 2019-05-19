package com.uberpalform.sparkexport

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit
/***
  * EmailSender class is responsible for sending export email to end users
  * @param spark - spark session
  */

class EmailSender (spark : SparkSession) extends LazyLogging{

  val customerEmailColumn = "email"
  val customerNameColumn = "first_name"
  val customerSurnameColumn = "second_name"

  /***
    * sendEmails is a function which is sending emails to end users. Function has a predefined  email template which is send in a email body. Function process exports one by one changing requests status after email was send.
    * @param exportsDF - export data which should be send to customers
    * @param customerIdColumn - customer identification number column will be used as a rowkey during status change.
    * @param hbaseNamespace - request hbase table namespace
    * @param hbaseTable - request hbase table name
    * @param hbaseColumnFamily - request hbase column family
    */

  def sendEmails(exportsDF : DataFrame, customerIdColumn : String, hbaseNamespace : String, hbaseTable: String, hbaseColumnFamily : String): Unit = {

    val emailList = exportsDF.select(customerEmailColumn).distinct().collect().map(_.getString(0))

    val hbaseWriterObject = new HbaseWriter(hbaseNamespace, hbaseTable, hbaseColumnFamily)

    exportsDF.cache() // caching because exportDF will be used multiple times

    logger.info("Starting sending emails ... ")
    emailList.foreach(emailAddress => {

      logger.info(s"Starting sending email on $emailAddress ... ")
      val customerExport = exportsDF.filter(exportsDF(customerEmailColumn) === emailAddress)

      val customerName : String = customerExport.select(customerNameColumn).collect()(0).getString(0)
      val customerSurname : String = customerExport.select(customerSurnameColumn).collect()(0).getString(0)

      val customerExportCleaned = customerExport
        .drop(customerEmailColumn)
        .drop(customerNameColumn)
        .drop(customerSurnameColumn)

      val emailText =
        s"""
           |Dear $customerName $customerSurname,
           |
           |Uber platform kindly sending you your data export which can be found in the attachment to this letter. Please, reach Uber platform in case of any questions.
           |
           |Kind regards,
           |Uber gdpr team
           |""".stripMargin

      logger.info(s"Sending email to $emailAddress")


       // TODO:include email sending functionality (Requires SMTP server). For testing purpose was implemented printing email text and export in log.

      println(emailText)
      customerExportCleaned.show()



      logger.info("Changing request status in hbase ...  ")
      hbaseWriterObject.writeRequestHbase(customerExportCleaned.select(customerIdColumn)
        .withColumnRenamed(customerIdColumn, "rowkey")
        .withColumn("status", lit("completed"))
      )

    })

  }
}
