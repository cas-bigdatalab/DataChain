package cn.cnic.bigdatalab.entity


import cn.cnic.bigdatalab.utils.{PropertyUtil, FileUtil}
import jodd.util.PropertiesUtil

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.tools.nsc.Properties
import scala.util.parsing.json.JSON

/**
  * Created by Flora on 2016/6/23.
  */
class Schema {

  var name:String = _
  var driver:String = _
  var db:String = _
  var table:String = _
  var columns:ArrayBuffer[String] = _
  var attachment: Map[String, String] = _


  def getName(): String={
    name
  }

  def setName(name:String): Unit ={
    this.name = name
    this
  }
  def getDriver(): String ={
    driver
  }

  def setDriver(driver: String): Schema ={
    this.driver = driver
    this
  }

  def getDb(): String ={
    db
  }

  def setDb(db: String): Schema ={
    this.db = db
    this
  }

  def getTable(): String ={
    table
  }

  def setTable(table: String): Schema ={
    this.table = table
    this
  }

  def getColumns(): ArrayBuffer[String] ={
    columns
  }

  def setColumns(columns: ArrayBuffer[String]): Schema ={
    this.columns = columns
    this
  }

  def getAttachment(): Map[String, String] = {
    attachment
  }

  def setAttachment(attachment: Map[String, String]): Schema = {
    this.attachment = attachment
    this
  }

  def columnsToString(): String ={
    /*val columnsBuffer:StringBuffer = new StringBuffer()

    var i = 1
    for((key,value) <- columns){
      columnsBuffer.append(key + " " + value)
      if(i < columns.size){
        columnsBuffer.append(", ")
      }
      i = i + 1
    }*/
    val columnsBuffer:StringBuffer= new StringBuffer()
    for(i <- 0 until columns.size){
      columnsBuffer.append(columns(i).replace(":", " "))
      if(i < columns.length - 1){
        columnsBuffer.append(", ")
      }
    }
    columnsBuffer.toString

  }
  def columnsFieldToString(): String ={
    /*val columnsBuffer:StringBuffer = new StringBuffer()

    var i = 1
    for((key,value) <- columns){
      columnsBuffer.append(key + " " + value)
      if(i < columns.size){
        columnsBuffer.append(", ")
      }
      i = i + 1
    }*/
    val columnsBuffer:StringBuffer= new StringBuffer()
    for(i <- 0 until columns.size){
      columnsBuffer.append(columns(i).split(":")(0))
      if(i < columns.length - 1){
        columnsBuffer.append(", ")
      }
    }
    columnsBuffer.toString

  }

  def partitonFieldToString(): String = {
    val partitionBuffer:StringBuffer = new StringBuffer()
    partitionBuffer.append("")
    if(!attachment.isEmpty && !attachment.get("partitions").isEmpty){
      val partitions = attachment.get("partitions").get.split(",")
      for(i <- 0 until partitions.length){
        partitionBuffer.append(partitions(i).stripMargin.split(" ").filterNot(_.equals(""))(0))
        if(i < partitions.length - 1){
          partitionBuffer.append(", ")
        }
      }
    }
    partitionBuffer.toString
  }

}

object Schema{

  def parserJsonFile(fileName:String):Schema={

    val filePath = PropertyUtil.getPropertyValue("json_path") + "/table/" +fileName+".json"

    val schema = parserJson(FileUtil.fileReader(filePath),"tableSpec")
    schema.setName(fileName)
    schema
  }

  def parserJson(jsonStr:String, tableName:String):Schema={

    val mapping = JSON.parseFull(jsonStr).get
    val map: Map[String, Any] = mapping.asInstanceOf[Map[String, Any]].get(tableName).get.asInstanceOf[Map[String, Any]]

    parserMap(map)

  }

  def parserMap(map: Map[String, Any]):Schema={

    val schema = new Schema()

    //val map: Map[String, Any] = mapping.asInstanceOf[Map[String, Any]].get(tableName).get.asInstanceOf[Map[String, Any]]

    //driver
    assert(!map.get("driver").get.asInstanceOf[String].isEmpty)
    val driver = map.get("driver").get.asInstanceOf[String]

    //db
    if(!map.get("db").isEmpty){
      val db = map.get("db").get.asInstanceOf[String]
      schema.setDb(db)
    }

    //table
    assert(!map.get("table").get.asInstanceOf[String].isEmpty)
    val table = map.get("table").get.asInstanceOf[String]

    //columns
    assert(!map.get("columns").get.asInstanceOf[List[String]].isEmpty)
    val columnsArrayBuffer = ArrayBuffer[String]()
    val columnsList = map.get("columns").get.asInstanceOf[List[String]]
    columnsArrayBuffer.appendAll(columnsList)

    schema.setDriver(driver).setTable(table).setColumns(columnsArrayBuffer)

    //attachment
    if(!map.get("attachment").isEmpty){
      val attachment = map.get("attachment").get.asInstanceOf[Map[String, String]]
      schema.setAttachment(attachment)
    }

    schema

  }


  def parseMultiSchema(map: Map[String, Any]) : List[Schema]={

    val schemaListBuffer : ListBuffer[Schema] = new ListBuffer[Schema]()

    //val schemaList : ArrayList[Schema] = new ArrayList[Schema]()

    map.keys.foreach(key => {
      val schemaFileName = map.get(key).get.asInstanceOf[String]
      //val sc = parserMap(schemaMap)
      val sc = parserJsonFile(schemaFileName)
      schemaListBuffer.append(sc)
    })

    schemaListBuffer.toList.asInstanceOf[List[Schema]]

  }

}