package cn.cnic.bigdatalab.compute.realtime.utils
import cn.cnic.bigdatalab.transformer.Transformer
import cn.cnic.bigdatalab.utils.FieldTypeUtil

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe

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

  def invoker(objectName: String, methodName: String, arg: Any*) = {
    val runtimeMirror = universe.runtimeMirror(Class.forName(objectName).getClassLoader)
    val moduleSymbol = runtimeMirror.moduleSymbol(
      Class.forName(objectName))

    val targetMethod = moduleSymbol.typeSignature
      .members
      .filter(x => x.isMethod && x.name.toString == methodName)
      .head
      .asMethod

    runtimeMirror.reflect(runtimeMirror.reflectModule(moduleSymbol).instance)
      .reflectMethod(targetMethod)(arg:_*)
  }

  def invokeStaticMethod(className: String, methodName: String, args: Object*): Unit ={
    val ownerClass = Class.forName(className)

    val argsClass: Array[Class[_]] = new Array[Class[_]](args.length)

    val i: Int = 0
    for (i <- 0 until args.length) {
      argsClass.update(i, args(i).getClass)
    }

    val method = ownerClass.getMethod(methodName, argsClass:_*)

    method.invoke(null, args:_*)
  }

}




