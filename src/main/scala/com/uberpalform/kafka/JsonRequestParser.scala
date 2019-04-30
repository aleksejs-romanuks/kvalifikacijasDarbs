package com.uberpalform.kafka

import java.util.NoSuchElementException

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.typesafe.scalalogging.LazyLogging
import scala.util.parsing.json.JSON

/**
  * JsonRequestParser is a class designed to parse incoming json requests
  * @param spark - Spark session
  */
class JsonRequestParser(val spark : SparkSession)  extends java.io.Serializable with LazyLogging{
  private case class inRequests(
    customerId : String,
    role : String,
    status : String,
    requestDateTime : String
  )

  private def parseJson(inStr : String) : inRequests = {
    val result = JSON.parseFull(inStr)
    result match {
      case Some(e) => {
        val request = e.asInstanceOf[Map[String, String]]
        try {
          inRequests(
            request("customerId"),
            request("role"),
            request("status"),
            request("requestDateTime")
          )
        }catch {
          case e : NoSuchElementException => {
            logger.warn(s"Corrupted input request $request ($e)")
            inRequests(null, null, null, null)
          }
        }
      }
      case None => {
        logger.info(s"Bad request $inStr. Request will be rejected. ")
        inRequests(null, null, null, null)
      }
    }
  }

  /***
    * parseJsonRDDtoDF function is parsing rdd of jsons in the dataframe.
    * @param inRDD - rdd of jsons
    * @return - dataframe with schema ([customerId, StringType], [requestDateTime, StringType])
    */
  def parseJsonRDDtoDF(inRDD : RDD[String]) : DataFrame = {
    val parsedRdd = inRDD.map(parseJson).filter(_.customerId != null)
    spark.sqlContext.createDataFrame(parsedRdd)
  }

}
