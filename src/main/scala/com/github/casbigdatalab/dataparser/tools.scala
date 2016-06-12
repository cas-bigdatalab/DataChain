package com.github.casbigdatalab.datachain.dataparser

/**
 * Created by cnic-liliang on 2016/6/6.
 */

import com.sun.xml.internal.fastinfoset.util.StringArray
import scala.collection.immutable.HashMap
import scala.util.parsing.json._
import scala.io.Source

object tools{
  def jsonfile2str(jsonfile:String):String = {
    var str = ""
    val file = Source.fromFile(jsonfile)
    val iter = file.buffered
    while (iter.hasNext) {
      val line = iter.head
      str += line
      iter.next()
    }
    file.close()
    val out = str
    out
  }

  def jsonfile2JsonMap(jsonfile:String):Any = {
    var str = ""
    val file = Source.fromFile(jsonfile)
    val iter = file.buffered
    while (iter.hasNext) {
      val line = iter.head
      str += line
      iter.next()
    }
    file.close()

    val json:Option[Any] = JSON.parseFull(str)
    //check whether mapping are correctly coded, otherwise throw exception
    val check = json match {
      case Some(xmap: Map[String, Any]) => {}
    }

    json.get
  }

  def jsonStr2HashMap(jsonstr:String): HashMap[String, String] = {
    val json:Option[Any] = JSON.parseFull(jsonstr)
    //check whether mapping are correctly coded, otherwise throw exception
    val check = json match {
      case Some(xmap: Map[String, Any]) => {}
    }
    json.get.asInstanceOf[HashMap[String, String]]
  }

  //csv
  def jsonMap2Delimiter(mapping:Any):Char = {
    val jmmap: Map[String, Any] = mapping.asInstanceOf[Map[String, Any]].get("mappingSpec").get.asInstanceOf[Map[String, Any]]
    val delimiter = jmmap.get("delimiter").get.toString.trim.charAt(0)
    delimiter
  }
  //
  def jsonMap2SchemaList(mapping:Any):List[String] = {
    val jmmap:Map[String, Any] = mapping.asInstanceOf[Map[String, Any]].get("mappingSpec").get.asInstanceOf[Map[String, Any]]
    val schemaList = jmmap.get("dimensionsMap").get.asInstanceOf[Map[String, Any]].get("dimensions").get.asInstanceOf[List[String]]
    schemaList
  }
}
