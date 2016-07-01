 package cn.cnic.bigdatalab.transformer

import java.util

 import scala.collection.mutable.ArrayBuffer

 /**
 * Created by cnic-liliang on 2016/5/25.
 */

trait common {
   def transform(msg:String): ArrayBuffer[Any]
   def getSchema():ArrayBuffer[String]
}

