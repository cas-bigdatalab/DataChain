package cn.cnic.bigdatalab.transformer

/**
 * Created by cnic-liliang on 2016/6/6.
 */

import cn.cnic.bigdatalab.utils.FieldTypeUtil
import com.sun.xml.internal.fastinfoset.util.StringArray
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import scala.collection.immutable.HashMap
import scala.collection.immutable.HashMap.HashTrieMap
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

  def jsonMap2Columns(mapping:Any):List[String] = {
    val jmmap:Map[String, Any] = mapping.asInstanceOf[Map[String, Any]].get("mappingSpec").get.asInstanceOf[Map[String, Any]]
    val colsList = jmmap.get("columns").get.asInstanceOf[List[String]]
    colsList
  }

  def jsonMap2ColumnTypes(mapping:Any):List[String] = {
    val jmmap:Map[String, Any] = mapping.asInstanceOf[Map[String, Any]].get("mappingSpec").get.asInstanceOf[Map[String, Any]]
    val colsList = jmmap.get("columnsType").get.asInstanceOf[List[String]]
    colsList
  }

  //json
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

  //schemaList
  def jsonMap2SchemaList(mapping:Any):List[String] = {
    val jmmap:Map[String, Any] = mapping.asInstanceOf[Map[String, Any]].get("mappingSpec").get.asInstanceOf[Map[String, Any]]
    val schemaList = jmmap.get("dimensionsMap").get.asInstanceOf[Map[String, Any]].get("dimensions").get.asInstanceOf[List[String]]
    schemaList
  }

  // convertTimestamp
  def jsonMap2ConvertTimestamp(mapping:Any):Option[Any] = {
    val jmmap:Map[String, Any] = mapping.asInstanceOf[Map[String, Any]].get("mappingSpec").get.asInstanceOf[Map[String, Any]]
    jmmap.get("dimensionsMap").get.asInstanceOf[Map[String, Any]].get("convertTimestamp")
  }

  //regex
  def jsonMap2RegexStr(mapping:Any):String = {
    val jmmap: Map[String, Any] = mapping.asInstanceOf[Map[String, Any]].get("mappingSpec").get.asInstanceOf[Map[String, Any]]
    val regexstr = jmmap.get("pattern").get.toString.trim
    regexstr
  }

  def valueDataType(typeName: String, srcValue:String, tmap:TMapping):Any =
    typeName match {
      case "timestamp" => {
        var timestamp:String = srcValue.trim
        if(timestamp.charAt(0) =='[') timestamp = timestamp.substring(1, timestamp.size - 1)

        val inputpattern = tmap.convertTimestamp.get("inputFormats").get
        val outputpattern = tmap.convertTimestamp.get("outputFormat").get
        DateTime.parse(timestamp, DateTimeFormat.forPattern(inputpattern)).toString(outputpattern)
      }
      case _       =>  FieldTypeUtil.parseDataType(typeName, srcValue)
    }


}
