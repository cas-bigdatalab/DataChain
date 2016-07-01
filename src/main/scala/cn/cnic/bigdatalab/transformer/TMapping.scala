package cn.cnic.bigdatalab.transformer

import scala.collection.mutable.ArrayBuffer
/**
  * Created by Flora on 2016/6/23.
  */

class TMapping(mapjson:String){
  var mapType:String = "csvString"
  var columns: ArrayBuffer[String] = new ArrayBuffer[String]()
  var dimensions: ArrayBuffer[String] = new ArrayBuffer[String]()
  var pattern:String = ""
  var delimiter:Char = ',' //default is ,

  //init
  if(mapjson.length > 0) init(mapjson)

  def init(mapjson:String) = {
    val mapping = tools.jsonfile2JsonMap(mapjson)
    val map: Map[String, Any] = mapping.asInstanceOf[Map[String, Any]].get("mappingSpec").get.asInstanceOf[Map[String, Any]]

    //type
    if(map.get("type").get.asInstanceOf[String].isEmpty == false) mapType = map.get("type").get.asInstanceOf[String]
    else throw  new IllegalArgumentException("type is not exist in " + mapjson)
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
    //pattern
    if(map.get("pattern").isEmpty == false) pattern = map.get("pattern").get.toString
    //
  }
}
