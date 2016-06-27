package cn.cnic.bigdatalab.Task

import cn.cnic.bigdatalab.entity.Schema
import cn.cnic.bigdatalab.utils.PropertyUtil
import com.github.casbigdatalab.datachain.transformer.Transformer

/**
  * Created by cnic on 2016/6/21.
  */
abstract class BaseTask() {
  protected val taskParams: TaskParams = new TaskParams
  protected val scheduler: Scheduler = new Scheduler
  def run

}

class RealTimeTask(sql:String, topic:String, schema:Schema, transformer: Transformer) extends BaseTask(){
  override def run(): Unit ={
    //init compute params
    val realTimeParams = List(taskParams.getDuration(), taskParams.getTopic(topic),
    taskParams.getKafkaParams(), taskParams.getSchemaColumns(schema),
    taskParams.getSchemaName(schema), taskParams.getCreateTableSql(schema),
      sql, taskParams.getSqlType(schema.getDriver()))

    //init app params
    val appParam = Map("class" -> PropertyUtil.getPropertyValue("realtime_class"),
      "path" -> PropertyUtil.getPropertyValue("realtime_path"))

    //init spark params
    val sparkParams = Map("master" -> PropertyUtil.getPropertyValue("master"), "executor-memory" -> PropertyUtil.getPropertyValue("executor-memory"),
    "total-executor-cores" -> PropertyUtil.getPropertyValue("total-executor-cores"))

    scheduler.deploy(realTimeParams, appParam, sparkParams)

  }
}

class OfflineTask(sql:String, srcSchema:Schema, destSchema:Schema) extends BaseTask(){
  override def run(): Unit ={
    //init offline task params
    val offlineParams = List(
      taskParams.getCreateTableSql(srcSchema),
      taskParams.getSqlType(srcSchema.getDriver()),
      taskParams.getCreateTableSql(destSchema),
      taskParams.getSqlType(destSchema.getDriver()),
      sql)

    //init app params
    val appParam = Map("class" -> PropertyUtil.getPropertyValue("offline_class"),
      "path" -> PropertyUtil.getPropertyValue("offline_path"))

    //init spark params
    val sparkParams = Map("master" -> PropertyUtil.getPropertyValue("master"), "executor-memory" -> PropertyUtil.getPropertyValue("executor-memory"),
      "total-executor-cores" -> PropertyUtil.getPropertyValue("total-executor-cores"))

    scheduler.deploy(offlineParams, appParam, sparkParams)

  }

}

class StoreTask(topic:String, schema:Schema, transformer: Transformer) extends BaseTask(){
  override def run(): Unit ={
    //create task
    //scheduler.deploy

  }

}