package cn.cnic.bigdatalab.utils

import cn.cnic.bigdatalab.entity.Schema

/**
  * Created by Flora on 2016/6/23.
  */
object SqlUtil {

  val INSERTKEYWORD = List("insert", "into", "table", "overwrite" ,"directory", "local")

  //CREATE TEMPORARY TABLE %tablename% USING %using% OPTIONS ( url '%url%', dbtable '%table%')
  def mysql(schema: Schema): String ={
    val temp = PropertyUtil.getPropertyValue("mysql_create_sql")
    val using = PropertyUtil.getPropertyValue("mysql_driver")
    val url = PropertyUtil.getPropertyValue("mysql_url") + schema.getDb() + "?user=" +
      PropertyUtil.getPropertyValue("mysql_user") + "&password=" + PropertyUtil.getPropertyValue("mysql_password")
    val dbtable = schema.getTable()
    val schemaName = schema.getName()
    //return temp.replace("%tablename%", dbtable).replace("%using%", using).replace("%url%", url).replace("%table%", dbtable).stripMargin
    return temp.replace("%tablename%", schemaName).replace("%using%", using).replace("%url%", url).replace("%table%", dbtable).stripMargin
  }

  //CREATE TEMPORARY TABLE %tablename% ( %columns% ) USING %using% OPTIONS ( host '%host%', database '%db%', collection '%table%')
  def mongo(schema: Schema): String ={
    val temp = PropertyUtil.getPropertyValue("mongo_create_sql")
    val using = PropertyUtil.getPropertyValue("mongo_driver")
    val host = PropertyUtil.getPropertyValue("mongo_host")
    val dbtable = schema.getTable()
    val db = schema.getDb()
    val schemaName = schema.getName()
    val columnsStr = schema.columnsToString()

    return temp.replace("%tablename%", schemaName).replace("%columns%", columnsStr).replace("%using%", using).replace("%host%", host).replace("%db%", db).replace("%table%", dbtable).stripMargin
  }

  def hive(schema: Schema): String ={
    val temp = PropertyUtil.getPropertyValue("hive_create_sql")
    val dbtable = schema.getTable()
    val columns =  schema.columnsToString()
    var result = temp.replace("%tablename%", dbtable).replace("%columns%", columns).stripMargin
    if(schema.getAttachment()!= null && !schema.getAttachment().isEmpty){
      for((key, value) <- schema.getAttachment()){
        result = result.replace(s"%$key%", value)
      }
    }else{
      result = result.replace(", %partitions%", "")
    }
    result
  }
  def hive_offline(schema: Schema): String ={
    val temp = PropertyUtil.getPropertyValue("hive_create_sql_offline")
    val dbtable = schema.getTable()
    val columns =  schema.columnsToString()
    temp.replace("%tablename%", dbtable).replace("%columns%", columns).stripMargin
  }
  def hhase(schema: Schema): String ={
    val temp = PropertyUtil.getPropertyValue("hbase_create_sql")
    val using = PropertyUtil.getPropertyValue("hbase_driver")
    val columns = schema.columnsToString()
    val hiveDbtable = schema.getTable()
    val hbaseDbtable = schema.getTable()

    val hbaseColumns: StringBuffer = new StringBuffer()
    hbaseColumns.append(":key,")

    //TODO: need to test
    /*schema.getColumns().keySet.filter(_ != "id").foreach(key =>{
      hbaseColumns.append("cf").append(key).append(":").append(key).append(",")
    } )*/
    for(i <- 0 until schema.getColumns().size){
      val field = schema.getColumns()(i).split(":")(0)
      if(!field.equals("id")){
        hbaseColumns.append("cf").append(field).append(":").append(field).append(",")
      }
    }

    hbaseColumns.deleteCharAt(hbaseColumns.length()-1)

    return temp.replace("%tablename%", hiveDbtable).replace("%columns%", columns).
      replace("%using%", using).replace("%hbase_columns%", hbaseColumns).
      replaceAll("%hbase_tablename%", hbaseDbtable)

  }

  //CREATE TEMPORARY TABLE %tablename% USING %using% OPTIONS ( zkhost  '%zkhost%', collection '%table%', soft_commit_secs '1', gen_uniq_key 'true', fields '%columns%')
  def solr(schema: Schema): String ={
    val temp = PropertyUtil.getPropertyValue("solr_create_sql")
    val using = PropertyUtil.getPropertyValue("solr_driver")
    val zkhost = PropertyUtil.getPropertyValue("solr_zkhost")
    val columns = schema.columnsFieldToString()
    val solrDbtable = schema.getTable()
    val schemaName = schema.getName()

    return temp.replace("%tablename%", schemaName).replace("%using%", using).
      replace("%zkhost%", zkhost).replace("%table%",solrDbtable).replace("%columns%", columns)

  }

  //CREATE TEMPORARY TABLE  %tablename% ( %columns% ) USING %using% OPTIONS (address '%address%', key '%table%')
  def memcache(schema: Schema): String = {
    val temp = PropertyUtil.getPropertyValue("memcache_create_sql")
    val using = PropertyUtil.getPropertyValue("memcache_driver")
    val address = PropertyUtil.getPropertyValue("memcache_address")
    val columns = schema.columnsToString()
    val memDbtable = schema.getTable()
    val schemaName = schema.getName()

    return  temp.replace("%tablename%", schemaName).replace("%columns%", columns).replace("%using%", using).
      replace("%address%", address).replace("%table%",memDbtable)
  }

  def getInsertSchema(sql: String, schemaList: List[Schema]): Schema = {
    val insertStart = sql.toLowerCase().indexOf("insert")
    val words = sql.substring(insertStart, sql.length).split(" ").filterNot(_.equals("")).filterNot({word =>
      INSERTKEYWORD.contains(word.toLowerCase)
    })
    if(words.isEmpty){
      throw new IllegalArgumentException("sql not include insert.")
    }
    val insertTable = words(0)
    var insertSchema: Schema = null
    for(schema <- schemaList){
      if(insertTable.equals(schema.getTable()) || insertTable.equals(schema.getName())){
        insertSchema = schema
      }
    }
    if(insertSchema == null){
      throw new IllegalArgumentException("Table has no driver.")
    }

    insertSchema

  }

  def parseSql(sql: String, schema: Schema):(String, String)={
    val index = sql.indexOf("(")
    val partitionSql = sql.substring(0, index+1) + "mp=%mp%, " + sql.substring(index+1)
    val hive_merge = PropertyUtil.getPropertyValue("hive_merge")
    val hive_truncate = PropertyUtil.getPropertyValue("hive_truncate")
    var attachSql: StringBuffer = new StringBuffer()
    if(schema.getAttachment()!= null && !schema.getAttachment().isEmpty && !schema.getAttachment().get("partitions").isEmpty){
      attachSql.append(hive_merge.replaceAll("%tablename%", schema.getTable()).replace("%columns%", schema.columnsFieldToString())
        .replaceAll("%partition_columns%", schema.partitonFieldToString)).append(PropertyUtil.getPropertyValue("create_sql_separator"))
        .append(hive_truncate.replace("%tablename%", schema.getTable()))
    }else{
      attachSql.append(hive_merge.replaceAll("%tablename%", schema.getTable()).replace("%columns%", schema.columnsToString()))
        .append(PropertyUtil.getPropertyValue("create_sql_separator"))
        .append(hive_truncate.replace("%tablename%", schema.getTable()))
    }
    (partitionSql, attachSql.toString)
  }

}
