package com.github.casbigdatalab.datachain.transformer

/**
 * Created by cnic-liliang on 2016/6/3.
 */

import java.util

// example about defining customized_parser
object customized_transformer {
  class customized_transformer(mapping_conf : String) extends common {
    val jmapping = tools.jsonfile2JsonMap(mapping_conf)
    val schemaList =tools.jsonMap2SchemaList(jmapping)

    override def transform(msg:String): util.ArrayList[String] = {
      val result = new util.ArrayList[String]()
      //extract and assembly
      for(item <- schemaList) {
        //result.add(Map(item -> msg))
        result.add(item + ": " + msg)
      }
      result
    }
  }

  def main(agrs: Array[String]): Unit = {
    val mapping_conf = "conf\\" + "regexMapping.json"
    val msg = "custom-defined-message"
    val customparser = new transformer(mapping_conf, new customized_transformer(mapping_conf))
    println("msg: " + msg)
    println("parse result: : " + customparser.transform((msg)))
  }
}