package cn.cnic.bigdatalab.utils

import cn.cnic.bigdatalab.entity.Schema

/**
  * Created by Flora on 2016/6/23.
  */
object SqlUtil {

  def mysql(schema: Schema): String ={
    val temp = PropertyUtil.getPropertyValue("mysql_create_sql")
    val using = PropertyUtil.getPropertyValue("mysql_driver")
    val url = PropertyUtil.getPropertyValue("mysql_url") + schema.getDb() + "?user=" +
      PropertyUtil.getPropertyValue("mysql_user") + "&password=" + PropertyUtil.getPropertyValue("mysql_password")
    val dbtable = schema.getTable()
    return temp.replace("%using%", using).replace("%url%", url).replace("%table%", dbtable).stripMargin
  }

  def mongo(schema: Schema): String ={
    val temp = PropertyUtil.getPropertyValue("mongo_create_sql")
    val using = PropertyUtil.getPropertyValue("mongo_driver")
    val host = PropertyUtil.getPropertyValue("mongo_host")
    val dbtable = schema.getTable()
    val db = schema.getDb()
    val columns: StringBuffer = new StringBuffer()
    for((key, value) <- schema.getColumns()){
      columns.append(key).append(" ").append(value).append(", ")
    }
    columns.delete(columns.length()-2, columns.length()-1)
    return temp.replace("%columns%", columns.toString).replace("%using%", using).replace("%host%", host).replace("%db%", db).replace("%table%", dbtable).stripMargin
  }

}
