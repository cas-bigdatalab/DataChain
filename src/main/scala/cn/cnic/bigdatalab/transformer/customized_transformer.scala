 package cn.cnic.bigdatalab.transformer

/**
 * Created by cnic-liliang on 2016/6/3.
 */

import java.util

 import scala.collection.mutable.ArrayBuffer
 import cn.cnic.bigdatalab.utils.FieldTypeUtil

 // example about defining customized_parser
object customized_transformer {
  class customized_transformer(mapping_conf : String) extends common {
    println(mapping_conf)

    val jmapping = tools.jsonfile2JsonMap(mapping_conf)
    val schemaList =tools.jsonMap2SchemaList(jmapping)

    override def getSchema():ArrayBuffer[String] = {
      var row = ArrayBuffer[String]()
      for(item <- schemaList) {
        val value = item
        row += item
      }
      row
    }
    override def transform(msg:String): ArrayBuffer[Any] = {
      val result = new ArrayBuffer[Any]()
      //extract and assembl
      for(item <- schemaList) {
        //result.add(Map(item -> msg))
        val value = FieldTypeUtil.parseDataType(item.trim.split(":")(1), msg)
        result += value
      }
      result
    }
  }

  def main(agrs: Array[String]): Unit = {
    val mapping_conf = "conf\\" + "customMapping.json"
    val msg = "custom-defined-message"
    val customparser = new Transformer(new customized_transformer(mapping_conf))
    println("msg: " + msg)
    println("parse result: : " + customparser.transform((msg)))
  }
}