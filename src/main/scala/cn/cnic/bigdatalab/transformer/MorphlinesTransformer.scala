package cn.cnic.bigdatalab.transformer

import java.util
import java.util.ArrayList

import cn.cnic.bigdatalab.transformer.morphlines._
import cn.cnic.bigdatalab.utils.FieldTypeUtil
import org.kitesdk.morphline.api.Record
import scala.collection.mutable.ArrayBuffer

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

  var moResult = new Record
  var firstProcess = true

  override def getSchema(): ArrayBuffer[String] = {
    tmap.dimensions
  }

  override def multiLineTransform(msg: String): ArrayBuffer[ArrayBuffer[Any]] = {
    collector.reset()
    morphline.process(msg)

    var mulResult:ArrayBuffer[ArrayBuffer[Any]] = new ArrayBuffer[ArrayBuffer[Any]]()
    var cnt:Int = 0;
    while (cnt < collector.getRecords.size()) {
      var moResult = collector.getRecords.get(cnt)
      println(moResult)

      moResult.removeAll("_attachment_body")
      moResult.removeAll("_attachment_mimetype")
      moResult.removeAll("bodybak")

      val result = new ArrayBuffer[Any]()
      //extract and assembl
      if(tmap.dimensions.size > 1) {
        for (item <- tmap.dimensions) {
          val feildValue = moResult.get(item.trim.split(":")(0)).get(0)
          //val value = FieldTypeUtil.parseDataType(item.trim.split(":")(1), feildValue.toString)
          val value = tools.valueDataType(item.trim.split(":")(1), feildValue.toString, tmap)
          result += value
        }
      }
      else {
        //println(moResult.getFields.keys().toArray().length)
        for(key <- moResult.getFields.keys().toArray()) {
          //println(key + " <- " +moResult.get(key.toString).get(0))
          val value = moResult.get(key.toString).get(0)
          result += value
        }
      }
      mulResult += result

      cnt += 1
    }

    mulResult
  }

  override def transform(msg: String): ArrayBuffer[Any] = {
    collector.reset()
    morphline.process(msg)
    moResult = collector.getRecords.get(0)

    //remove additional maps
    moResult.removeAll("_attachment_body")
    moResult.removeAll("_attachment_mimetype")
    moResult.removeAll("bodybak")
    //println(moResult)
    //schema
//    println(moResult.getFields.keys().toArray().length)
//    println(moResult.getFields.keys().toArray()(2))

    val result = new ArrayBuffer[Any]()
    //extract and assembl
    if(tmap.dimensions.size > 1) {
      for (item <- tmap.dimensions) {
        val feildValue = moResult.get(item.trim.split(":")(0)).get(0)
        //val value = FieldTypeUtil.parseDataType(item.trim.split(":")(1), feildValue.toString)
        val value = tools.valueDataType(item.trim.split(":")(1), feildValue.toString, tmap)
        result += value
      }
    }
    else {
      //println(moResult.getFields.keys().toArray().length)
      for(key <- moResult.getFields.keys().toArray()) {
        //println(key + " <- " +moResult.get(key.toString).get(0))
      }
      //val feildValue = moResult.get(item.trim.split(":")(0)).get(0)
    }
    result
  }
}
