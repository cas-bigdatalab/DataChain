package cn.cnic.bigdatalab.transformer

/**
 * Created by cnic-liliang on 2016/6/3.
 */

import java.util
import scala.collection.mutable.ArrayBuffer

object TransformerCreater {
  var mapping_type = "csvMapping"
  //Input is json mapping file
  def creater(mapping_json: String): TransformerBase = {
    val tmap = new TMapping(mapping_json)
    mapping_type = tmap.mapType
    val parser =
      mapping_type match {
        case "csvMapping" => new CSVTransformer(tmap)
        case "jsonMapping" => new JsonTransformer(tmap)
        case "regexMapping" => new RegexTransformer(tmap)
        case "morphlinesMapping" => new MorphlinesTransformer(tmap)
        case _ => throw new IllegalArgumentException( mapping_type + " could not be found, please use the customMapping!")
      }
    parser
  }
  //Input is Mapping
  def creater(tmap: TMapping): TransformerBase = {
    mapping_type = tmap.mapType
    val parser =
      mapping_type match {
        case "csvMapping" => new CSVTransformer(tmap)
        case "jsonMapping" => new JsonTransformer(tmap)
        case "regexMapping" => new RegexTransformer(tmap)
        case "morphlinesMapping" => new MorphlinesTransformer(tmap)
        case _ => throw new IllegalArgumentException( mapping_type + " could not be found, please use the customMapping!")
      }
    parser
  }
}

class Transformer(transform: TransformerBase) {

  def this(mapping: String) = {
    this(TransformerCreater.creater(mapping))
  }
  def this(map:TMapping) = {
    this(TransformerCreater.creater(map))
  }

  def transform(msg: String): ArrayBuffer[Any] = {
    transform.transform(msg)
  }

  def getSchema():ArrayBuffer[String] = {
    transform.getSchema()
  }
}

