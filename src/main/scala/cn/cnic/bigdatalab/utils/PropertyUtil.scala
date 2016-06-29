package cn.cnic.bigdatalab.utils

import java.io.{FileInputStream, InputStream}
import java.util.Properties


/**
  * Created by Flora on 2016/6/23.
  */

object PropertyUtil {
  private val prop: Properties = new Properties()
  var fis: InputStream = null
  try{
    val path = Thread.currentThread().getContextClassLoader.getResource("config.properties").getPath
    //fis = this.getClass.getResourceAsStream("")
    prop.load(new FileInputStream(path))
  } catch{
    case ex: Exception => ex.printStackTrace()
  }

  def getPropertyValue(propertyKey: String): String ={
    val obj = prop.get(propertyKey)
    if(obj != null){
      return obj.toString
    }
    null
  }

  def getIntPropertyValue(propertyKey: String): Int ={
    val obj = prop.getProperty(propertyKey)
    if(obj != null){
      return obj.toInt
    }
    throw new NullPointerException
  }

  def main(args: Array[String]): Unit ={
    println(PropertyUtil.getPropertyValue("mysql_url"))
    try{
      println(PropertyUtil.getIntPropertyValue("ddd"))
    }catch {
      case ex: NullPointerException => println(null)
    }

  }

}
