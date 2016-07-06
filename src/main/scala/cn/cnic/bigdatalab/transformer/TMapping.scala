package cn.cnic.bigdatalab.transformer

import org.apache.spark.sql.types.MapType

import scala.collection.mutable.ArrayBuffer
/**
  * Created by Flora on 2016/6/23.
  */

class TMapping(mapconf: String) extends Serializable{
  var mapType:String = "csvString"
  var columns: ArrayBuffer[String] = new ArrayBuffer[String]()
  var dimensions: ArrayBuffer[String] = new ArrayBuffer[String]()
  var pattern:String = ""
  var delimiter:Char = ',' //default is ,
  var conf:String = ""
  var mapjsonconf:String = mapconf

  //init
  if(mapjsonconf.length > 0) init(mapjsonconf)

  def this() = {
    this("")
  }

  def setMapType(newMapType: String): TMapping = {
    mapType = newMapType
    this
  }

  def setColumns(colArrBuf: ArrayBuffer[String]): TMapping = {
    columns = colArrBuf
    this
  }

  def setDimensions(dimArrBuf: ArrayBuffer[String]): TMapping = {
    dimensions = dimArrBuf
    this
  }

  def setPattern(newPattern:String):TMapping = {
    pattern = newPattern
    this
  }

  def setDelimeter(newDelimeter:Char):TMapping = {
    delimiter = newDelimeter
    this
  }

  def setConf(newConf:String):TMapping = {
    conf = newConf
    this
  }

  def init(mapjson:String) = {
    val mapping = Tools.jsonfile2JsonMap(mapjson)
    val map: Map[String, Any] = mapping.asInstanceOf[Map[String, Any]].get("mappingSpec").get.asInstanceOf[Map[String, Any]]

    //type
    if(map.get("type").get.asInstanceOf[String].isEmpty == false) mapType = map.get("type").get.asInstanceOf[String]
    else throw  new IllegalArgumentException("type is not exist in " + mapjson)

    //conf
    conf =
      mapType match {
        case "morphlinesMapping" => map.get("conf").get.asInstanceOf[String]
        case _ => ""
      }

    //columns
    if(map.get("columns").isEmpty == false) {
      var value:ArrayBuffer[String] = new ArrayBuffer[String]()
      val loccolumns =Tools.jsonMap2Columns(mapping)
      for(item <- loccolumns) value += item.toString
      columns = value
    }
    //delimiter
    if(map.get("delimiter").isEmpty == false) delimiter = map.get("delimiter").get.toString.trim.charAt(0)
    //dimensions
    val svalue = new ArrayBuffer[String]()
    val schemalist = Tools.jsonMap2SchemaList(mapping)
    for(item <- schemalist) svalue += item.toString
    dimensions = svalue
    //pattern
    if(map.get("pattern").isEmpty == false) pattern = map.get("pattern").get.toString
    //
  }
}
