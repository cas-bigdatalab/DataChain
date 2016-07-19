package cn.cnic.bigdatalab.transformer

import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
/**
  * Created by Flora on 2016/6/23.
  */

class TransformerMapping(mapconf: String) extends Serializable{
  var mapType:String = "csvString"
  var columns: ArrayBuffer[String] = new ArrayBuffer[String]()
  var dimensions: ArrayBuffer[String] = new ArrayBuffer[String]()
  var convertTimestamp:HashMap[String, String] = new HashMap[String, String]()
  var pattern:String = ""
  var delimiter:Char = ',' //default is ,
  var conf:String = ""

  var multiline:Boolean = false

  var mapjsonconf:String = mapconf
  //init
  if(mapjsonconf.length > 0) init(mapjsonconf)

  def this() = {
    this("")
  }

  def setMapType(newMapType: String): TransformerMapping = {
    mapType = newMapType
    this
  }

  def setColumns(colArrBuf: ArrayBuffer[String]): TransformerMapping = {
    columns = colArrBuf
    this
  }

  def setDimensions(dimArrBuf: ArrayBuffer[String]): TransformerMapping = {
    dimensions = dimArrBuf
    this
  }

  def setPattern(newPattern:String):TransformerMapping = {
    pattern = newPattern
    this
  }

  def setDelimeter(newDelimeter:Char):TransformerMapping = {
    delimiter = newDelimeter
    this
  }

  def setConf(newConf:String):TransformerMapping = {
    conf = newConf
    this
  }

  def setMultilines(ismultiline:Boolean):TransformerMapping = {
    multiline = ismultiline
    this
  }

  def init(mapjson:String) = {
    val mapping = tools.jsonfile2JsonMap(mapjson)
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
      val loccolumns =tools.jsonMap2Columns(mapping)
      for(item <- loccolumns) value += item.toString
      columns = value
    }
    //delimiter
    if(map.get("delimiter").isEmpty == false) delimiter = map.get("delimiter").get.toString.trim.charAt(0)
    //dimensions
    val svalue = new ArrayBuffer[String]()
    val schemalist = tools.jsonMap2SchemaList(mapping)
    for(item <- schemalist) svalue += item.toString
    dimensions = svalue
    //convertTimestamp
    val convert = tools.jsonMap2ConvertTimestamp(mapping)
    if(convert.nonEmpty) {
      convertTimestamp.put("field", convert.get.asInstanceOf[Map[String, Any]].get("field").get.toString)
      convertTimestamp.put("inputFormats", convert.get.asInstanceOf[Map[String, Any]].get("inputFormats").get.toString)
      convertTimestamp.put("inputTimezone", convert.get.asInstanceOf[Map[String, Any]].get("inputTimezone").get.toString)
      convertTimestamp.put("outputFormat", convert.get.asInstanceOf[Map[String, Any]].get("outputFormat").get.toString)
      convertTimestamp.put("outputTimezone", convert.get.asInstanceOf[Map[String, Any]].get("outputTimezone").get.toString)
    }
    //pattern
    if(map.get("pattern").isEmpty == false) pattern = map.get("pattern").get.toString
    //
  }
}
