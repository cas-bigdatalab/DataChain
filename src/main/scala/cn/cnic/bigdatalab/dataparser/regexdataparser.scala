package cn.cnic.bigdatalab.dataparser

import java.util
import java.util.regex.Pattern;

/**
 * Created by cnic-liliang on 2016/6/13.
 */

class regexdataparser(mapping_conf : String) extends common{
  val jmapping = tools.jsonfile2JsonMap(mapping_conf)
  val schemaList =tools.jsonMap2SchemaList(jmapping)

  def parse(msg:String): util.ArrayList[Map[String, Any]] = {
    //regular expression
    val patternstr = tools.jsonMap2RegexStr(jmapping)
    val pattern = Pattern.compile(patternstr)
    val m = pattern.matcher(msg)
    //columns
    val columnslist = tools.jsonMap2Columns(jmapping)

    val result = new util.ArrayList[Map[String, Any]]()
    //traverse
    if (m.find()){
      //m.group(0) refers to msg, so begining from 1 to m.groupCount()
      var i = 1
      while(i <= m.groupCount()) {
        //extract
        if(schemaList.contains(columnslist(i-1))) {
          val map= Map(columnslist(i-1).toString -> m.group(i).toString)
          result.add(map)
        }
        i += 1
      }
    }
    result
  }
}
