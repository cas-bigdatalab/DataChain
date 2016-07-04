package cn.cnic.bigdatalab.transformer

import cn.cnic.bigdatalab.transformer.morphlines._
import cn.cnic.bigdatalab.utils.FieldTypeUtil
import scala.collection.mutable.ArrayBuffer

import java.io.IOException
import java.io.InputStream
import java.util.ArrayList
import java.util.Collection
import java.util.HashMap
import java.util.List
import java.util.Map
import java.util.Queue
import java.util.concurrent.ConcurrentLinkedQueue

import org.kitesdk.morphline.api.Command
import org.kitesdk.morphline.api.Record
import org.kitesdk.morphline.base.Fields

/**
 * Created by cnic-liliang on 2016/7/1.
 */

class MorphlinesTransformer(tmap : TMapping) extends TransformerBase {
  println(tmap.conf)
  val morphlineFile = tmap.conf
  val morphline:MorphlineHandlerImpl = new MorphlineHandlerImpl()
  val collector:Collector = new Collector()
  morphline.setFinalChild(collector)
  morphline.configure(morphlineFile)

  override def getSchema(): ArrayBuffer[String] = {
    tmap.dimensions
  }

  override def transform(msg: String): ArrayBuffer[Any] = {
    collector.reset()
    morphline.process(msg)
    var moResult = collector.getRecords.get(0)
    //remove additional maps
    moResult.removeAll("_attachment_body")
    moResult.removeAll("_attachment_mimetype")
    moResult.removeAll("bodybak")
    //schema
//    println(moResult.getFields.keys().toArray().length)
//    println(moResult.getFields.keys().toArray()(2))

    val result = new ArrayBuffer[Any]()
    //extract and assembl
    for (item <- tmap.dimensions) {
      val feildValue = moResult.get(item.trim.split(":")(0)).get(0)
      val value = FieldTypeUtil.parseDataType(item.trim.split(":")(1), feildValue.toString)
      result += value
    }
    result
  }
}
