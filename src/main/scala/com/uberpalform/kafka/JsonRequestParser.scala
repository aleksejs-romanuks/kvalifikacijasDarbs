package com.uberpalform.kafka

import java.util.NoSuchElementException

import com.uberpalform.kafka.test.inRequests
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.parsing.json.JSON

class JsonRequestParser(val spark : SparkSession)  extends java.io.Serializable {
  private case class inRequests(
    customerId: String,
    requestDateTime: String
  )

  private def parseJson(inStr : String) : inRequests = {
    val result = JSON.parseFull(inStr)
    result match {
      case Some(e) => {
        val request = e.asInstanceOf[Map[String, String]]
        try {
          inRequests(request("customerId"), request("requestDateTime"))
        }catch {
          case e : NoSuchElementException => {
            println(s"Corrupted input request $request ($e)")
            inRequests(null, null)
          }
        }
      }
      case None => inRequests(null, null)
    }
  }
  def parseJsonRDDtoDF(inRDD : RDD[String]) : DataFrame = {
    val parsedRdd = inRDD.map(parseJson(_)).filter(_.customerId != null).filter(_.requestDateTime != null)
    spark.sqlContext.createDataFrame(parsedRdd)
  }

}
