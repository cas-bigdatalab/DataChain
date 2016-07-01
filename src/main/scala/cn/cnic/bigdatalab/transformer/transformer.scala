package cn.cnic.bigdatalab.transformer

/**
 * Created by cnic-liliang on 2016/6/3.
 */

import java.util

import cn.cnic.bigdatalab.transformer.tools

import scala.collection.mutable.ArrayBuffer

object transformerCreater {
  var mapping_type = "csvMapping"

  def mapping(mapjson: String):Mapping = {
    val mapping = tools.jsonfile2JsonMap(mapjson)
    val map: Map[String, Any] = mapping.asInstanceOf[Map[String, Any]].get("mappingSpec").get.asInstanceOf[Map[String, Any]]

    val resultMap:Mapping = new Mapping
    //type
    if(map.get("type").get.asInstanceOf[String].isEmpty == false) resultMap.mapType = map.get("type").get.asInstanceOf[String]
    else throw  new IllegalArgumentException("type is not exist in " + mapjson)
    //columns
    if(map.get("columns").isEmpty == false) {
      val value = new ArrayBuffer[String]()
      val columns =tools.jsonMap2Columns(mapping)
      for(item <- columns)  value += item.toString
      resultMap.columns = value
    }
    //delimiter
    if(map.get("delimiter").isEmpty == false) resultMap.delimiter = map.get("delimiter").get.toString.trim.charAt(0)
    //dimensions
    val svalue = new ArrayBuffer[String]()
    val schemalist = tools.jsonMap2SchemaList(mapping)
    for(item <- schemalist) svalue += item.toString
    resultMap.dimensions = svalue
    //pattern
    if(map.get("pattern").isEmpty == false) resultMap.pattern = map.get("pattern").get.toString
    //
    resultMap
  }
  //Input is json mapping file
  def creater(mapping_json: String): common = {
    val tmap:Mapping = mapping(mapping_json)
    mapping_type = tmap.mapType
    val parser =
      mapping_type match {
        case "csvMapping" => new csvtransformer(tmap)
        case "jsonMapping" => new jsontransformer(tmap)
        case "regexMapping" => new regextransformer(tmap)
        case _ => throw new IllegalArgumentException( mapping_type + " could not be found, please use the customMapping!")
      }
    parser
  }
  //Input is Mapping
  def creater(tmap: Mapping): common = {
    mapping_type = tmap.mapType
    val parser =
      mapping_type match {
        case "csvMapping" => new csvtransformer(tmap)
        case "jsonMapping" => new jsontransformer(tmap)
        case "regexMapping" => new regextransformer(tmap)
        case _ => throw new IllegalArgumentException( mapping_type + " could not be found, please use the customMapping!")
      }
    parser
  }
}

class Transformer(parser: common) {
  def this(mapping: String) = {
    this(transformerCreater.creater(mapping))
  }
  def this(map:Mapping){
    this(transformerCreater.creater(map))
  }

  def transform(msg: String): ArrayBuffer[Any] = {
    parser.transform(msg)
  }

  def getSchema():ArrayBuffer[String] = {
    parser.getSchema()
  }
}
