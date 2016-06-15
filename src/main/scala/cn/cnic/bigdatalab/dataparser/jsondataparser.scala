package cn.cnic.bigdatalab.dataparser

import java.util

import tools

import scala.collection.immutable.HashMap
import scala.collection.mutable

/**
 * Created by cnic-liliang on 2016/6/12.
 */
class jsondataparser(mapping_conf : String) extends common {
  val jmapping = tools.jsonfile2JsonMap(mapping_conf)
  val schemaList =tools.jsonMap2SchemaList(jmapping)

  def parse(msg:String): util.ArrayList[Map[String, Any]] = {
    val jsonMsg:HashMap[String, Any] = tools.jsonStr2HashMap(msg)

    val result = new util.ArrayList[Map[String, Any]]()
    //extract
    for(item <- schemaList) {
      val map= Map(item.toString -> jsonMsg.get(item.toString).get.asInstanceOf[String])
      result.add(map)
    }
    result
  }
}
