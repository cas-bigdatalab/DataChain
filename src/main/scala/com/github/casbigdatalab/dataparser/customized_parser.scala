package com.github.casbigdatalab.datachain.dataparser

/**
 * Created by cnic-liliang on 2016/6/3.
 */

import java.util

// example about defining customized_parser
object customized_parse {
  class customized_parser(mapping_conf : String) extends common {
    val jmapping = tools.jsonfile2JsonMap(mapping_conf)
    val schemaList =tools.jsonMap2SchemaList(jmapping)

    override def parse(msg:String): util.ArrayList[Map[String, Any]] = {
      val result = new util.ArrayList[Map[String, Any]]()
      //extract and assembly
      for(item <- schemaList) {
        result.add(Map(item -> msg))
      }
      result
    }
  }

  def main(agrs: Array[String]): Unit = {
    val mapping_conf = "conf\\" + "regexMapping.json"
    val msg = "custom-defined-message"
    val customparser = new dataparser(mapping_conf, new customized_parser(mapping_conf))
    println("msg: " + msg)
    println("parse result: : " + customparser.parse((msg)))
  }
}