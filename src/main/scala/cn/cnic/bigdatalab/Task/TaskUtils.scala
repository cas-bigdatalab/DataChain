package cn.cnic.bigdatalab.Task

import cn.cnic.bigdatalab.entity.Schema
import cn.cnic.bigdatalab.utils.{PropertyUtil, SqlUtil}

/**
  * Created by Flora on 2016/6/23.
  */
object TaskUtils {
  private val param_delimiter = "\""

  def wrapDelimiter(name: String): String ={
    if(name != null && !name.equals("") && !name.equals(param_delimiter))
       return param_delimiter + name + param_delimiter
    name
  }

  //kafkaParams
  def getKafkaParams(): String ={
    val params: StringBuffer = new StringBuffer()
    params.append("zookeeper.connect->").append(PropertyUtil.getPropertyValue("zookeeper.connect")).append(";")
    params.append("group.id->").append(PropertyUtil.getPropertyValue("group.id"))
    wrapDelimiter(params.toString)
  }

  //topics
  def getTopic(topic: String): String ={
    val params: StringBuffer = new StringBuffer()
    params.append(topic).append(":").append(1)
    wrapDelimiter(params.toString)
  }

  //schema columns
  def getSchemaColumns(schema: Schema): String = {
    val params: StringBuffer = new StringBuffer()
    val columns = schema.getColumns()
    for((key, value) <- columns){
      params.append(key).append(":").append(value).append(",")
    }
    params.deleteCharAt(params.length()-1)
    wrapDelimiter(params.toString)
  }

  //spark streaming duration
  def getDuration(): String ={
    PropertyUtil.getPropertyValue("duration")
  }

  //table name
  def getSchemaName(schema: Schema): String ={
    wrapDelimiter(schema.getTable())
  }

  //table type
  def getSchemaDriver(schema: Schema): String = {
    wrapDelimiter(schema.getDriver())
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
      return wrapDelimiter(SqlUtil.mysql(schema))
    }else if(sqlType.equals("mongo")){
      return wrapDelimiter(SqlUtil.mongo(schema))
    }else if(sqlType.equals("hive")){
      return wrapDelimiter(SqlUtil.hive(schema))
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
//    println(TaskUtils.getSchemaColumns(schema))
    println(TaskUtils.getCreateTableSql(schema))
  }
}
