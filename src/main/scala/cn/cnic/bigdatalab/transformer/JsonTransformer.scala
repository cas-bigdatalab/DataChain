 package cn.cnic.bigdatalab.transformer

import java.util
import scala.collection.immutable.HashMap
 import scala.collection.mutable.ArrayBuffer
 import cn.cnic.bigdatalab.utils.FieldTypeUtil

 /**
 * Created by cnic-liliang on 2016/6/12.
 */
class JsonTransformer(tmap : TransformerMapping) extends TransformerBase{
   val schema = tmap.dimensions

   def getSchema():ArrayBuffer[String] = {
     schema
   }

   def multiLineTransform(msg: String):ArrayBuffer[ArrayBuffer[Any]] = {
     return new ArrayBuffer[ArrayBuffer[Any]]()
   }

  def transform(msg:String): ArrayBuffer[Any] = {
    val jsonMsg:HashMap[String, Any] = tools.jsonStr2HashMap(msg)

    val result = new ArrayBuffer[Any]()
    //extract
    for(item <- schema) {
      //val map= Map(item.toString -> jsonMsg.get(item.toString).get.asInstanceOf[String])
      //val value = FieldTypeUtil.parseDataType(item.toString.split(":")(1), jsonMsg.get(item.toString.split(":")(0)).get.asInstanceOf[String])
      val value = tools.valueDataType(item.toString.split(":")(1), jsonMsg.get(item.toString.split(":")(0)).get.asInstanceOf[String].trim, tmap)
      result += value
     }
    result
  }
}
