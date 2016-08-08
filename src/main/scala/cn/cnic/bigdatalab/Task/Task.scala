package cn.cnic.bigdatalab.task

import cn.cnic.bigdatalab.task.factory.{ExternalTask, SQLTask, TaskBean, TaskTypeFactory}
import cn.cnic.bigdatalab.entity.Schema

import scala.collection.mutable.ArrayBuffer

/**
  * Created by cnic on 2016/6/21.
  */
abstract class BaseTask() {
  protected var scheduler: Scheduler = _

  def run

  def cancel(name: String)

}

class RealTimeTask(taskInstance: TaskBean) extends BaseTask() {
  this.scheduler = new RealTimeScheduler

  override def run(): Unit = {
    scheduler.deploy(taskInstance)

  }

  override def cancel(name: String): Unit = {
    scheduler.cancel(name)
  }
}

class OfflineTask(taskInstance: TaskBean) extends BaseTask() {
  this.scheduler = new OfflineScheduler

  override def run(): Unit = {
    scheduler.deploy(taskInstance)
  }

  override def cancel(name: String): Unit ={
    scheduler.cancel(name)
  }

}

class StoreTask(taskInstance: TaskBean) extends BaseTask() {
  this.scheduler = new RealTimeScheduler

  override def run(): Unit = {
    scheduler.deploy(taskInstance)

  }

  override def cancel(name: String): Unit = ???
}

object TaskTest {
  def main(args: Array[String]): Unit = {
    val topic = "user"
    val schema = new Schema()
    schema.setDriver("mongo")
    schema.setDb("test")
    schema.setTable("user")
    schema.setColumns(ArrayBuffer("id:Int","name:String", "age:String"))

    val sql = "select * from user"

    val task: TaskBean = new SQLTask().initRealtime("test_task", sql, topic, schema, schema, "mapping")

    val realTimeTask = new RealTimeTask(task)
    realTimeTask.run()

    val topic1 = "user1"
    val schema1 = new Schema()
    schema1.setDriver("mongo")
    schema1.setDb("test1")
    schema1.setTable("user1")
    schema1.setColumns(ArrayBuffer("id:Int","name:String", "age:String"))

    val sql1 = "select * from user"

    val task1: TaskBean = new SQLTask().initOffline("test_task1", sql, schema, schema)
    task1.interval= 5

    val offlineTask = new OfflineTask(task1)
    offlineTask.run()

    Thread.sleep(20*1000)

    offlineTask.cancel(task1.taskType+"_"+task1.name)

  }

}


object TaskFactory{
  def apply(kind: String, taskBean: TaskBean) = kind match {
    case "offline" => new OfflineTask(taskBean)
    case "realtime" => new RealTimeTask(taskBean)
    case "store" => new StoreTask(taskBean)
  }

}