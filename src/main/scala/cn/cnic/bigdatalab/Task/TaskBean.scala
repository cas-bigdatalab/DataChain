package cn.cnic.bigdatalab.Task

import cn.cnic.bigdatalab.entity.Schema
import cn.cnic.bigdatalab.utils.PropertyUtil

import scala.util.parsing.json.JSON

/**
  * Created by duyuanyuan on 2016/6/24.
  */
class TaskBean() {
  private var name: String = _
  private var taskType: String = _
  private var priority: Int = _
  private var interval: Long = _
  private var appParams: List[String] = _
  private var taskParams: Map[String, String] = _
  private var sparkParams: Map[String, String] = _

  def initRealtime(name: String, sql: String, topic: String, srcSchema: Schema, destSchema: Schema, mapping:String): TaskBean ={
    this.name = name
    this.taskType = "realtime"

    //init app params
    this.appParams = List(TaskUtils.getDuration(), TaskUtils.getTopic(topic),
      TaskUtils.getKafkaParams(), TaskUtils.getSchemaColumns(srcSchema),
      TaskUtils.getSchemaName(srcSchema), TaskUtils.getCreateTableSql(destSchema),
      TaskUtils.wrapDelimiter(sql), mapping, TaskUtils.getSqlType(destSchema.getDriver()))

    //init task params
    this.taskParams = Map("class" -> PropertyUtil.getPropertyValue("realtime_class"),
      "path" -> PropertyUtil.getPropertyValue("realtime_path"))

    //init spark params
    this.sparkParams = Map("master" -> PropertyUtil.getPropertyValue("master"), "executor-memory" -> PropertyUtil.getPropertyValue("executor-memory"),
      "total-executor-cores" -> PropertyUtil.getPropertyValue("total-executor-cores"))

    this

  }

  def initOffline(name: String, sql: String, srcSchema: Schema, destSchema: Schema, interval: Long = -1): TaskBean ={
    this.name = name
    this.taskType = "offline"
    this.interval = interval

    //init app params
    this.appParams = List(
      TaskUtils.getCreateTableSql(srcSchema),
      TaskUtils.getSchemaDriver(srcSchema),
      TaskUtils.getCreateTableSql(destSchema),
      TaskUtils.getSchemaDriver(destSchema),
      TaskUtils.wrapDelimiter(sql)
    )
    //init task params
    this.taskParams = Map("class" -> PropertyUtil.getPropertyValue("offline_class"),
      "path" -> PropertyUtil.getPropertyValue("offline_path"))

    //init spark params
    this.sparkParams = Map("master" -> PropertyUtil.getPropertyValue("master"), "executor-memory" -> PropertyUtil.getPropertyValue("executor-memory"),
      "total-executor-cores" -> PropertyUtil.getPropertyValue("total-executor-cores"))

    this

  }

  def initStore(name: String, topic: String, srcSchema: Schema, destSchema: Schema, mapping:String): TaskBean ={
    this.name = name
    this.taskType = taskType
    val sql = "insert into table " + destSchema.getTable() + " select * from " + srcSchema.getTable()

    //init app params
    this.appParams = List(TaskUtils.getDuration(), TaskUtils.getTopic(topic),
      TaskUtils.getKafkaParams(), TaskUtils.getSchemaColumns(srcSchema),
      TaskUtils.getSchemaName(srcSchema), TaskUtils.getCreateTableSql(destSchema),
      TaskUtils.wrapDelimiter(sql), mapping, TaskUtils.getSqlType(destSchema.getDriver()))

    //init task params
    this.taskParams = Map("class" -> PropertyUtil.getPropertyValue("realtime_class"),
      "path" -> PropertyUtil.getPropertyValue("realtime_path"))

    //init spark params
    this.sparkParams = Map("master" -> PropertyUtil.getPropertyValue("master"), "executor-memory" -> PropertyUtil.getPropertyValue("executor-memory"),
      "total-executor-cores" -> PropertyUtil.getPropertyValue("total-executor-cores"))

    this

  }

  def setTaskType(taskType: String): Unit ={
    this.taskType = taskType
  }

  def getTaskType(): String ={
    this.taskType
  }

  def setName(name: String): Unit ={
    this.name = name
  }

  def getName(): String ={
    this.name
  }

  def setPriority(priority: Int): Unit ={
    this.priority = priority
  }

  def getPriority(): Int ={
    this.priority
  }

  def setInterval(interval: Long): Unit ={
    this.interval = interval
  }

  def getInterval(): Long ={
    this.interval
  }

  def setAppParams(params: List[String]): Unit ={
    this.appParams = params
  }

  def getAppParams(): List[String] ={
    this.appParams
  }

  def setTaskParams(params: Map[String, String]): Unit ={
    this.taskParams = params
  }

  def getTaskParams(): Map[String, String] = {
    this.taskParams
  }

  def setSparkParams(params: Map[String, String]): Unit ={
    this.sparkParams = params
  }

  def getSparkParams(): Map[String, String] ={
    this.sparkParams
  }

}

object TaskBean{

  def parseJson(jsonStr: String): TaskBean = {

    val mapping = JSON.parseFull(jsonStr).get
    val map: Map[String, Any] = mapping.asInstanceOf[Map[String, Any]].get("Task").get.asInstanceOf[Map[String, Any]]

    parseMap(map)

  }

  def parseMap(map: Map[String, Any]): TaskBean ={

    val taskBean: TaskBean = new TaskBean()

    //name
    assert(!map.get("name").get.asInstanceOf[String].isEmpty)
    val name = map.get("name").get.asInstanceOf[String]

    //taskType
    assert(!map.get("taskType").get.asInstanceOf[String].isEmpty)
    val taskType = map.get("taskType").get.asInstanceOf[String]

    //sql
    assert(!map.get("sql").get.asInstanceOf[String].isEmpty)
    val sql = map.get("sql").get.asInstanceOf[String]

    //srcTable
    assert(!map.get("srcTable").get.asInstanceOf[Map[String, Any]].isEmpty)
    val srcSchema = Schema.parserMap(map.get("srcTable").get.asInstanceOf[Map[String, Any]])

    //destTable
    assert(!map.get("destTable").get.asInstanceOf[Map[String, Any]].isEmpty)
    val destSchema = Schema.parserMap(map.get("destTable").get.asInstanceOf[Map[String, Any]])


    taskType match {
      case "realtime" =>{

        //topic for realtime task
        assert(!map.get("topic").get.asInstanceOf[String].isEmpty)
        val topic = map.get("topic").get.asInstanceOf[String]

        taskBean.initRealtime(name,sql,topic,srcSchema,destSchema,"")

      }
      case "offline" =>{
        var interval:Long = -1
        if(!map.get("interval").get.asInstanceOf[String].isEmpty)
          interval = map.get("interval").get.asInstanceOf[String].toLong
        taskBean.initOffline(name,sql, srcSchema, destSchema, interval)
      }
    }

    taskBean

  }

}
