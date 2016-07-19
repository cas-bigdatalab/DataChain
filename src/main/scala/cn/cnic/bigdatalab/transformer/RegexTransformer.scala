 package cn.cnic.bigdatalab.transformer

import java.util
import java.util.regex.Pattern;
 import cn.cnic.bigdatalab.utils.FieldTypeUtil
 import org.joda.time.DateTime
 import org.joda.time.format.DateTimeFormat

 import scala.collection.mutable.ArrayBuffer

 /**
 * Created by cnic-liliang on 2016/6/13.
 */

class RegexTransformer(tmap : TransformerMapping) extends TransformerBase with Serializable{
   val schema = tmap.dimensions

   def getSchema():ArrayBuffer[String] = {
     schema
   }

   def multiLineTransform(msg: String):ArrayBuffer[ArrayBuffer[Any]] = {
     return new ArrayBuffer[ArrayBuffer[Any]]()
   }

  def transform(msg:String): ArrayBuffer[Any] = {
    //regular expression
    val patternstr = tmap.pattern
    val pattern = Pattern.compile(patternstr)
    val m = pattern.matcher(msg)
    //columns
    val columns = tmap.columns
    //
    val result = new ArrayBuffer[Any]()
    //traverse
    if (m.find()){
      //m.group(0) refers to msg, so begining from 1 to m.groupCount()
      var i = 1
      while(i <= m.groupCount()) {
        //extract
        if(schema.toList.contains(columns(i-1))) {
          //val map= Map(columnslist(i-1).toString -> m.group(i).toString)
          //println(columns(i-1).toString.split(":")(1))
          val value = tools.valueDataType(columns(i-1).toString.split(":")(1), m.group(i).toString.trim, tmap)
          //println(value)
          result += value //.add(columnslist(i-1).toString + ": " + m.group(i).toString)
        }
        i += 1
      }
    }
    result
  }

}
