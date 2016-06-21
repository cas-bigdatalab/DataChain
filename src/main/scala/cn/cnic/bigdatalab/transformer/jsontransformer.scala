package com.github.casbigdatalab.datachain.transformer

import java.util
import scala.collection.immutable.HashMap

/**
 * Created by cnic-liliang on 2016/6/12.
 */
class jsontransformer(mapping_conf : String) extends common {
  val jmapping = tools.jsonfile2JsonMap(mapping_conf)
  val schemaList =tools.jsonMap2SchemaList(jmapping)

  def transform(msg:String): util.ArrayList[String] = {
    val jsonMsg:HashMap[String, Any] = tools.jsonStr2HashMap(msg)

    val result = new util.ArrayList[String]()
    //extract
    for(item <- schemaList) {
      //val map= Map(item.toString -> jsonMsg.get(item.toString).get.asInstanceOf[String])
      result.add(item.toString + ": " + jsonMsg.get(item.toString).get.asInstanceOf[String])
    }
    result
  }
}
