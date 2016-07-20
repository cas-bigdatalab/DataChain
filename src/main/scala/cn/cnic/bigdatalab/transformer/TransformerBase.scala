 package cn.cnic.bigdatalab.transformer

import java.util

 import scala.collection.mutable.ArrayBuffer

 /**
 * Created by cnic-liliang on 2016/5/25.
 */

trait TransformerBase extends Serializable{
   def transform(msg:String): ArrayBuffer[Any]
   def multiLineTransform(msg:String): ArrayBuffer[ArrayBuffer[Any]]
   def getSchema():ArrayBuffer[String]
}


