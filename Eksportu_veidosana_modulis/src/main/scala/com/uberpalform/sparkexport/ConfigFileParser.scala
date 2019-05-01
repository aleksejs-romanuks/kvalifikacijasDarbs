package com.uberpalform.sparkexport

import scala.xml.XML

class ConfigFileParser (xmlConfigFile : String) {
  private val xml = XML.loadString(xmlConfigFile)

  case class joinConfig(
                         tableName : String,
                         commonColumn : String,
                         exportColumns : String
                       )

  case class hbaseTableInfo(
                             hbaseTableName : String,
                             hbaseTableNamespace : String,
                             hbaseTableColumnFamily : String,
                             hbaseColumns : List[String],
                             hbaseFilter : String
                           )
  case class exportConfig(
                           emailColumn : String,
                           startingTable : (String, String),
                           joins : List[joinConfig],
                           outputPath : String
                         )

  def getHbaseTableInfo: hbaseTableInfo = {
    hbaseTableInfo(
      (xml \ "requests"  \ "hbaseTableName").text,
      (xml \ "requests"  \ "hbaseTableNamespace").text,
      (xml \ "requests"  \ "hbaseTableColumnFamily").text,
      (xml \ "requests"  \ "hbaseColumns" \ "column").map(x => x.text.trim).toList,
      (xml \ "requests"  \ "hbaseFilter").text
    )
  }
  def getExportConfig : exportConfig = {
    exportConfig(
      (xml \ "exportConfig"  \ "emailColumn").text,
      ((xml \ "exportConfig"  \ "startingPoint" \ "tableName").text, (xml \ "exportConfig"  \ "startingPoint" \ "exportColumns").text),
      (xml \ "exportConfig"  \ "joinTable").map(bla => joinConfig((bla \ "tableName").text, (bla \ "commonColumn").text, (bla \ "exportColumns").text)).toList,
      (xml \ "outputPath").text
    )

  }
}
