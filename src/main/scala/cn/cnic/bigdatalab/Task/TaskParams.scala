package cn.cnic.bigdatalab.Task

import cn.cnic.bigdatalab.entity.Schema
import cn.cnic.bigdatalab.utils.{PropertyUtil, SqlUtil}

/**
  * Created by Flora on 2016/6/23.
  */
class TaskParams {
  //kafkaParams
  def getKafkaParams(): String ={
    val params: StringBuffer = new StringBuffer()
    params.append("zookeeper.connect->").append(PropertyUtil.getPropertyValue("zookeeper.connect")).append(";")
    params.append("group.id->").append(PropertyUtil.getPropertyValue("group.id"))
    params.toString
  }

  //topics
  def getTopic(topic: String): String ={
    val params: StringBuffer = new StringBuffer()
    params.append(topic).append(":").append(1)
    params.toString
  }

  //schema columns
  def getSchemaColumns(schema: Schema): String = {
    val params: StringBuffer = new StringBuffer()
    val columns = schema.getColumns()
    for((key, value) <- columns){
      params.append(key).append(":").append(value).append(",")
    }
    params.deleteCharAt(params.length()-1)
    params.toString
  }

  //spark streaming duration
  def getDuration(): String ={
    PropertyUtil.getPropertyValue("duration")
  }

  //table name
  def getSchemaName(schema: Schema): String ={
    schema.getTable()
  }

  //Conform sqlType
  def getSqlType(driver: String): String ={
    val hivedb = PropertyUtil.getPropertyValue("hive_db").split(",")
    if(hivedb.contains(driver)){
      return "hive"
    }
    driver
  }

  //create table sql
  def getCreateTableSql(schema: Schema): String ={
    val sqlType = getSqlType(schema.getDriver())
    if(sqlType.equals("mysql")){
      return SqlUtil.mysql(schema)
    }else if(sqlType.equals("mongo")){
      return SqlUtil.mongo(schema)
    }
    null
  }

}

object test{
  def main(args: Array[String]): Unit ={
    val schema = new Schema()
    schema.setDriver("mongo")
    schema.setDb("test")
    schema.setTable("user")
    schema.setColumns(Map("id" -> "Int", "name" -> "String", "age" -> "String"))
//    println(new TaskParams().getSchemaColumns(schema))
    println(new TaskParams().getCreateTableSql(schema))
  }
}
