package cn.cnic.bigdatalab.compute.realtime

import cn.cnic.bigdatalab.transformer.Transformer
import cn.cnic.bigdatalab.utils.FieldTypeUtil
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import scala.collection.mutable.ArrayBuffer
import scala.tools.nsc.{Interpreter, Settings}

/**
  * Created by Flora on 2016/7/20.
  */
object Utils {

  def getSchema(transformer: Transformer): StructType ={
    // Generate the schema based on the string of schema
    var fields : ArrayBuffer[StructField] = new ArrayBuffer[StructField]()
    for (field <- transformer.getSchema()) {
      val Array(fieldName, fieldType) = field.split(":")
      fields += DataTypes.createStructField(fieldName, FieldTypeUtil.stringToDataType(fieldType), true)
    }
    DataTypes.createStructType(fields.toArray)
  }

}

class MyInterpreter(settings : scala.tools.nsc.Settings) extends Interpreter(settings : scala.tools.nsc.Settings) with Serializable

object InterpreterSingleton {

  @transient private var instance: MyInterpreter = _

  def getInstance(): MyInterpreter = {
    if (instance == null) {
      val settings = new Settings(str => println(str))
      settings.usejavacp.value = true
      instance = new MyInterpreter(settings)
    }
    instance
  }
}
