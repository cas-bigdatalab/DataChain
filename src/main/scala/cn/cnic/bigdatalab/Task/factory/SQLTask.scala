package cn.cnic.bigdatalab.task.factory

import cn.cnic.bigdatalab.entity.Schema
import cn.cnic.bigdatalab.task.TaskUtils
import cn.cnic.bigdatalab.utils.PropertyUtil

/**
  * Created by duyuanyuan on 2016/7/25.
  */

class SQLTask extends TaskBean{

  def initRealtime(name: String, sql: String, topic: String, srcSchema: Schema, destSchema: Schema, mapping:String, notificationTopic:String = ""): SQLTask ={
    this.taskType = "realtime"

    //init common params
    init(name, taskType+"_sql")

    //init app params
    this.appParams = List(this.taskType+"_"+name, TaskUtils.getDuration(), TaskUtils.getTopic(topic),
      TaskUtils.getKafkaParams(), TaskUtils.getSchemaName(srcSchema), TaskUtils.getCreateTableSql(destSchema),
      TaskUtils.wrapDelimiter(sql), mapping,
      TaskUtils.getTopic(notificationTopic),
      TaskUtils.getKafkaBrokerList())

    this

  }

  def initRealtimeMultiSchema(name: String, sql: String, topic: String, srcSchema: Schema, destSchema: List[Schema], mapping:String, notificationTopic:String = ""): SQLTask ={
    this.taskType = "realtime"

    //init common params
    init(name, taskType+"_sql")

    this.notificationTopic = notificationTopic

    //init temporary table description
    val temporaryTableDesc :StringBuilder = new StringBuilder()

    for(index <- 0 until destSchema.length){
      val schema = destSchema(index)
      temporaryTableDesc.append(TaskUtils.getCreateTableSqlNoWrap(schema)).append(PropertyUtil.getPropertyValue("create_sql_separator"))
    }
    temporaryTableDesc.delete(temporaryTableDesc.length - PropertyUtil.getPropertyValue("create_sql_separator").length, temporaryTableDesc.length)

    //transfer sql to real statement
    val sqlDescription = TaskUtils.transformSql(sql, destSchema: List[Schema])

    //init app params
    this.appParams = List(this.taskType+"_"+name, TaskUtils.getDuration(), TaskUtils.getTopic(topic),
      TaskUtils.getKafkaParams(), TaskUtils.getSchemaName(srcSchema), TaskUtils.wrapDelimiter(temporaryTableDesc.toString()),
      TaskUtils.wrapDelimiter(sqlDescription), mapping,
      TaskUtils.getTopic(notificationTopic),
      TaskUtils.getKafkaBrokerList())

    this

  }

  def initOffline(name: String, sql: String, srcSchema: Schema, destSchema: Schema, interval: Long = -1, expression: String = "", notificationTopic:String = ""): SQLTask ={
    this.taskType = "offline"
    this.interval = interval
    this.expression = expression
    this.notificationTopic = notificationTopic

    //  init common params
    init(name, taskType+"_sql")

    //init temporary table description
    val srcSqlDesc = TaskUtils.getCreateTableSqlNoWrap(srcSchema)
    val destSqlDesc = TaskUtils.getCreateTableSqlNoWrap(destSchema)
    val temporaryTableDesc = srcSqlDesc + PropertyUtil.getPropertyValue("create_sql_separator") + destSqlDesc

    //transfer sql to real statement
    val schemaList =List[Schema](srcSchema,destSchema)
    val sqlDescription = TaskUtils.transformSql(sql, schemaList: List[Schema])

    //init app params
    if (notificationTopic == ""){
      this.appParams = List(
        TaskUtils.wrapDelimiter(temporaryTableDesc.toString()),
        TaskUtils.wrapDelimiter(sqlDescription))
    }else{
      this.appParams = List(
        TaskUtils.wrapDelimiter(temporaryTableDesc.toString()),
        TaskUtils.wrapDelimiter(sqlDescription),
        TaskUtils.getTopic(notificationTopic),
        TaskUtils.getKafkaBrokerList()
      )
    }

    this

  }

  def initOfflineMultiSchema(name: String, sql: String, schemaList: List[Schema], interval: Long = -1, expression: String, notificationTopic:String = ""): SQLTask ={
    this.taskType = "offline"
    this.interval = interval
    this.expression = expression
    this.notificationTopic = notificationTopic

    //  init common params
    init(name, taskType+"_sql")

    //init temporary table description
    val temporaryTableDesc :StringBuilder = new StringBuilder()
    for(index <- 0 until schemaList.length){
      val schema = schemaList(index)
      val sqlDesc = TaskUtils.getCreateTableSqlNoWrap(schema)
      temporaryTableDesc.append(sqlDesc).append(PropertyUtil.getPropertyValue("create_sql_separator"))
    }
    temporaryTableDesc.delete(temporaryTableDesc.length - PropertyUtil.getPropertyValue("create_sql_separator").length, temporaryTableDesc.length)

    //transfer sql to real statement
    val sqlDescription = TaskUtils.transformSql(sql, schemaList: List[Schema])

    //init app params
    if (notificationTopic == ""){
      this.appParams = List(
        TaskUtils.wrapDelimiter(temporaryTableDesc.toString()),
        TaskUtils.wrapDelimiter(sqlDescription))
    }else{
      this.appParams = List(
        TaskUtils.wrapDelimiter(temporaryTableDesc.toString()),
        TaskUtils.wrapDelimiter(sqlDescription),
        TaskUtils.getTopic(notificationTopic),
        TaskUtils.getKafkaBrokerList()
      )
    }


    this
  }

  def initStore(name: String, topic: String, srcSchema: Schema, destSchema: Schema, mapping:String): SQLTask ={
    this.taskType = "store"
    var sql = "insert into table " + destSchema.getName() + " select * from " + srcSchema.getName()

    //  init common params
    init(name, taskType+"_sql")

    ////transfer sql to real statement
    val hive_db = PropertyUtil.getPropertyValue("hive_db")
    if(hive_db.contains(srcSchema.getDriver()))
      sql = sql.replace(srcSchema.getName(), srcSchema.getTable())

    if(hive_db.contains(destSchema.getDriver()))
      sql = sql.replace(destSchema.getName(), destSchema.getTable())


    //init app params
    this.appParams = List(this.taskType+"_"+name, TaskUtils.getDuration(), TaskUtils.getTopic(topic),
      TaskUtils.getKafkaParams(), TaskUtils.getSchemaName(srcSchema), TaskUtils.getCreateTableSql(destSchema),
      TaskUtils.wrapDelimiter(sql), mapping)

    this

  }

  def parseMap(map: Map[String, Any]): SQLTask ={

    //name
    assert(!map.get("name").get.asInstanceOf[String].isEmpty)
    val name = map.get("name").get.asInstanceOf[String]

    //taskType
    assert(!map.get("taskType").get.asInstanceOf[String].isEmpty)
    val taskType = map.get("taskType").get.asInstanceOf[String]

    //srcTable
    assert(!map.get("srcTable").get.asInstanceOf[Map[String, Any]].isEmpty)
    //val srcSchema = Schema.parserMap(map.get("srcTable").get.asInstanceOf[Map[String, Any]])
    val srcSchemaList : List[Schema] = Schema.parseMultiSchema(map.get("srcTable").get.asInstanceOf[Map[String, Any]])


    //destTable
    assert(!map.get("destTable").get.asInstanceOf[Map[String, Any]].isEmpty)
    //val destSchema = Schema.parserMap(map.get("destTable").get.asInstanceOf[Map[String, Any]])
    val destSchemaList : List[Schema] = Schema.parseMultiSchema(map.get("destTable").get.asInstanceOf[Map[String, Any]])


    taskType match {
      case "realtime" =>{

        //sql
        assert(!map.get("sql").get.asInstanceOf[String].isEmpty)
        val sql = map.get("sql").get.asInstanceOf[String]

        //topic for realtime task
        assert(!map.get("topic").get.asInstanceOf[String].isEmpty)
        val topic = map.get("topic").get.asInstanceOf[String]

        //mapping
        assert(!map.get("mapping").get.asInstanceOf[String].isEmpty)
        val mapping = map.get("mapping").get.asInstanceOf[String]

        val notificationTopic = map.getOrElse("notificationTopic", "").asInstanceOf[String]

        initRealtimeMultiSchema(name,sql,topic,srcSchemaList(0), destSchemaList, mapping, notificationTopic)

      }
      case "offline" =>{

        //sql
        assert(!map.get("sql").get.asInstanceOf[String].isEmpty)
        val sql = map.get("sql").get.asInstanceOf[String]

        //interval
        val interval = map.getOrElse("interval", "-1").asInstanceOf[String]

        //expression
        val expression = map.getOrElse("expression", "").asInstanceOf[String]

        val notificationTopic = map.getOrElse("notificationTopic", "").asInstanceOf[String]

        initOfflineMultiSchema(name,sql, srcSchemaList:::destSchemaList, interval.toLong, expression, notificationTopic)

      }
      case "store" =>{
        //topic for realtime task
        assert(!map.get("topic").get.asInstanceOf[String].isEmpty)
        val topic = map.get("topic").get.asInstanceOf[String]

        //mapping
        assert(!map.get("mapping").get.asInstanceOf[String].isEmpty)
        val mapping = map.get("mapping").get.asInstanceOf[String]

        initStore(name, topic, srcSchemaList(0), destSchemaList(0), mapping)
      }
    }

    this

  }

}

