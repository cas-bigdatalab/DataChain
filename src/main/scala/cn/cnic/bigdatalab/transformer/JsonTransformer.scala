 package cn.cnic.bigdatalab.transformer

import java.util
import scala.collection.immutable.HashMap
 import scala.collection.mutable.ArrayBuffer
 import cn.cnic.bigdatalab.utils.FieldTypeUtil

 /**
 * Created by cnic-liliang on 2016/6/12.
 */
class JsonTransformer(tmap : TMapping) extends TransformerBase {
   val schema = tmap.dimensions

   def getSchema():ArrayBuffer[String] = {
     schema
   }

  def transform(msg:String): ArrayBuffer[Any] = {
    val jsonMsg:HashMap[String, Any] = Tools.jsonStr2HashMap(msg)

    val result = new ArrayBuffer[Any]()
    //extract
    for(item <- schema) {
      //val map= Map(item.toString -> jsonMsg.get(item.toString).get.asInstanceOf[String])
      val value = FieldTypeUtil.parseDataType(item.toString.split(":")(1), jsonMsg.get(item.toString.split(":")(0)).get.asInstanceOf[String])
      result += value
     }

    result
  }
}