package com.uberpalform.sparkexport

import scala.xml.XML

/***
  * ConfigFileParser class is created to parse export job configuration.
  * @param xmlConfigFile - string configurations which needs to be parsed
  */
class ConfigFileParser (xmlConfigFile : String) {
  private val xml = XML.loadString(xmlConfigFile)

  case class joinConfig(
                         tableName : String,
                         condition : String,
                         exportColumns : List[String]
                       )

  case class hbaseTableInfo(
                             hbaseTableName : String,
                             hbaseTableNamespace : String,
                             hbaseTableColumnFamily : String,
                             hbaseColumns : List[String],
                             hbaseFilter : String,
                             hbaseRowkeyNewInstanceName : String
                           )

  case class exportConfig(
                           emailColumn : String,
                           startingTable : (String, List[String]),
                           joins : List[joinConfig]
                         )

  /***
    * getHbaseTableInfo function is extracting from xml file information about requests hbase table
    * @return - hbaseTableInfo case class object
    */
  def getHbaseTableInfo: hbaseTableInfo = {
    hbaseTableInfo(
      (xml \ "requests"  \ "hbaseTableName").text,
      (xml \ "requests"  \ "hbaseTableNamespace").text,
      (xml \ "requests"  \ "hbaseTableColumnFamily").text,
      (xml \ "requests"  \ "hbaseColumns" \ "column").map(x => x.text.trim).toList,
      (xml \ "requests"  \ "hbaseFilter").text,
      (xml \ "requests"  \ "hbaseRowkeyNewInstanceName").text
    )
  }

  /***
    * getExportConfig function is extraction from xml file information about export
    * @return - exportConfig case class object
    */
  def getExportConfig : exportConfig = {
    exportConfig(
      (xml \ "exportConfig"  \ "emailColumn").text,
      (
        (xml \ "exportConfig"  \ "startingPoint" \ "tableName").text,
        (xml \ "exportConfig"  \ "startingPoint" \ "exportColumns").text.replaceAll("\\s", "").split(",").toList
      ),
      (xml \ "exportConfig"  \ "joinTable").map(bla => joinConfig((bla \ "tableName").text, (bla \ "condition").text, (bla \ "exportColumns").text.replaceAll("\\s", "").split(",").toList)).toList
    )

  }
}
