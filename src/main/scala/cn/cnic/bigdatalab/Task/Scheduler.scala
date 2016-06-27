package cn.cnic.bigdatalab.Task

import akka.actor._
import akka.actor.Props
import akka.util.Timeout
import scala.concurrent.duration._
import akka.actor.ActorSystem
import cn.cnic.bigdatalab.entity.Schema
import cn.cnic.bigdatalab.utils.PropertyUtil

import scala.sys.process.Process

/**
  * Created by Flora on 2016/6/23.
  */

trait Scheduler{
  def deploy(taskInstance: TaskInstance)

  def execute(taskInstance: TaskInstance): Unit ={
    val command: StringBuffer = new StringBuffer()
    command.append("cd ").append(PropertyUtil.getPropertyValue("spark_home")).append(";")
      .append("./bin/spark-submit ").append("--class ").append(taskInstance.getTaskParams.get("class").get)
      .append(" --master ").append(taskInstance.getSparkParams.get("master").get)
      .append(" --executor-memory ").append(taskInstance.getSparkParams.get("executor-memory").get)
      .append(" --total-executor-cores ").append(taskInstance.getSparkParams.get("total-executor-cores").get)
      .append(" ").append(taskInstance.getTaskParams.get("path").get)

    command.append(taskInstance.getAppParams.mkString(" ", " ",""))

    val deployCmd =  "ssh root@" + PropertyUtil.getPropertyValue("spark_host") + " /bin/bash " + command

//    println(deployCmd)

    Process(Seq("bash","-c", deployCmd)).!
  }
}

class RealTimeScheduler extends Scheduler{

  override def deploy(taskInstance: TaskInstance): Unit ={
    this.execute(taskInstance)
  }
}

class OfflineActor(taskInstance: TaskInstance) extends Actor{

  def receive = {
    case task: OfflineScheduler => task.execute(taskInstance)
    case _ => ()
  }
}

class OfflineScheduler extends Scheduler{
  val system = ActorSystem("offline-timer")
  var tasks = Map[String, Cancellable]()
  override def deploy(taskInstance: TaskInstance): Unit ={
    import scala.concurrent.ExecutionContext.Implicits.global
    val act1 = system.actorOf(Props(new OfflineActor(taskInstance)), taskInstance.getTaskType()+"_"+taskInstance.getName())
    implicit val time = Timeout(5 seconds)
    val cancellable = system.scheduler.schedule(0 milliseconds,taskInstance.getInterval() seconds,act1,this)
    tasks += (taskInstance.getTaskType()+"_"+taskInstance.getName() -> cancellable)
  }

  def cancel(name: String): Unit ={
    tasks.get(name).get.cancel()
  }

}

object SchedulerTest{
  def main(args: Array[String]): Unit ={
    val scheduler = new RealTimeScheduler()

    val topic = "user"
    val schema = new Schema()
    schema.setDriver("mongo")
    schema.setDb("test")
    schema.setTable("user")
    schema.setColumns(Map("id" -> "Int", "name" -> "String", "age" -> "String"))

    val sql = "select * from user"

    val task: TaskInstance = new TaskInstance().init("test_task", "realtime", sql, topic, schema, "mapping")

    scheduler.deploy(task)

    val offlineScheduler = new OfflineScheduler()

    val topic1 = "user1"
    val schema1 = new Schema()
    schema1.setDriver("mongo")
    schema1.setDb("test1")
    schema1.setTable("user1")
    schema1.setColumns(Map("id" -> "Int", "name" -> "String", "age" -> "String"))

    val sql1 = "select * from user"

    val task1: TaskInstance = new TaskInstance().init("test_task1", "offline", sql, topic, schema, "mapping")
    task.setInterval(10)

    offlineScheduler.deploy(task)

  }
}