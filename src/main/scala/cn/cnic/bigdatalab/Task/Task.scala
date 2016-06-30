package cn.cnic.bigdatalab.Task

import cn.cnic.bigdatalab.entity.Schema
import cn.cnic.bigdatalab.utils.PropertyUtil
import com.github.casbigdatalab.datachain.transformer.Transformer

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

  override def cancel(name: String): Unit = ???
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
  override def run(): Unit = {
    //create task
    //scheduler.deploy


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
    schema.setColumns(Map("id" -> "Int", "name" -> "String", "age" -> "String"))

    val sql = "select * from user"

    val task: TaskBean = new TaskBean().init("test_task", "realtime", sql, topic, schema, schema, "mapping")

    val realTimeTask = new RealTimeTask(task)
    realTimeTask.run()

    val topic1 = "user1"
    val schema1 = new Schema()
    schema1.setDriver("mongo")
    schema1.setDb("test1")
    schema1.setTable("user1")
    schema1.setColumns(Map("id" -> "Int", "name" -> "String", "age" -> "String"))

    val sql1 = "select * from user"

    val task1: TaskBean = new TaskBean().init("test_task1", "offline", sql, topic, schema, schema, "mapping")
    task1.setInterval(5)

    val offlineTask = new OfflineTask(task1)
    offlineTask.run()

    Thread.sleep(20*1000)

    offlineTask.cancel(task1.getTaskType()+"_"+task1.getName())

  }

}