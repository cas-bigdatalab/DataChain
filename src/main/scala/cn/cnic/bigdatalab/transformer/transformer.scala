package cn.cnic.bigdatalab.transformer

/**
 * Created by cnic-liliang on 2016/6/3.
 */

import java.util
import scala.collection.mutable.ArrayBuffer

object transformerCreater {
  var mapping_type = "csvMapping"
  //Input is json mapping file
  def creater(mapping_json: String): common = {
    val tmap = new TMapping(mapping_json)
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
  def creater(tmap: TMapping): common = {
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
  def this(map:TMapping){
    this(transformerCreater.creater(map))
  }

  def transform(msg: String): ArrayBuffer[Any] = {
    parser.transform(msg)
  }

  def getSchema():ArrayBuffer[String] = {
    parser.getSchema()
  }
}
