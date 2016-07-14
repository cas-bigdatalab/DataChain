package cn.cnic.bigdatalab.Task

import akka.actor._
import akka.actor.Props
import akka.util.Timeout

import scala.concurrent.duration._
import akka.actor.ActorSystem
import cn.cnic.bigdatalab.entity.Schema
import cn.cnic.bigdatalab.utils.{PropertyUtil, SshUtil}

import scala.sys.process.Process

/**
  * Created by Flora on 2016/6/23.
  */

trait Scheduler{
  def deploy(taskInstance: TaskBean)

  def cancel(name: String)

  def execute(taskInstance: TaskBean): Unit ={
    val command: StringBuffer = new StringBuffer()
    command.append("cd ").append(PropertyUtil.getPropertyValue("spark_home")).append(";")
    command.append("./bin/spark-submit ").append("--class ").append(taskInstance.getTaskParams.get("class").get)
      .append(" --master ").append(taskInstance.getSparkParams.get("master").get)
      .append(" --executor-memory ").append(taskInstance.getSparkParams.get("executor-memory").get)
      .append(" --total-executor-cores ").append(taskInstance.getSparkParams.get("total-executor-cores").get)
      .append(" ").append(taskInstance.getTaskParams.get("path").get)


    command.append(taskInstance.getAppParams.mkString(" ", " ",""))

    //val deployCmd =  "ssh -t -t xjzhu@" + PropertyUtil.getPropertyValue("spark_host") + " &&  /bin/bash " + command
    val deployCmd =  command.toString

    println(deployCmd)


//    Process(Seq("bash","-c", deployCmd)).!

    SshUtil.exec(deployCmd, PropertyUtil.getPropertyValue("spark_host"), PropertyUtil.getPropertyValue("spark_host_user"),
      PropertyUtil.getPropertyValue("spark_host_password"))

  }
}

class RealTimeScheduler extends Scheduler{

  override def deploy(taskInstance: TaskBean): Unit ={
    this.execute(taskInstance)
  }

  override def cancel(name: String): Unit = ???
}

class OfflineActor(taskInstance: TaskBean) extends Actor{

  def receive = {
    case task: OfflineScheduler => task.execute(taskInstance)
    case _ => ()
  }
}

class OfflineScheduler extends Scheduler{
  val system = ActorSystem("offline-timer")
  var tasks = Map[String, Cancellable]()

  override def deploy(taskInstance: TaskBean): Unit ={
    import scala.concurrent.ExecutionContext.Implicits.global
    val act = system.actorOf(Props(new OfflineActor(taskInstance)), taskInstance.getTaskType()+"_"+taskInstance.getName())
    implicit val time = Timeout(5 seconds)

    val interval = taskInstance.getInterval()
    //val cancellable = system.scheduler.schedule(0 milliseconds,interval seconds, act, this)
    //val cancellable = system.scheduler.scheduleOnce(0 milliseconds, act, this)
    val cancellable = interval match {
      case -1 => system.scheduler.scheduleOnce(0 milliseconds, act, this)
      case x => system.scheduler.schedule(0 milliseconds,x seconds, act, this)
    }

    tasks += (taskInstance.getTaskType()+"_"+taskInstance.getName() -> cancellable)
  }

  override def cancel(name: String): Unit ={
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

    val task: TaskBean = new TaskBean().initRealtime("test_task", sql, topic, schema, schema, "mapping")

    scheduler.deploy(task)

    val offlineScheduler = new OfflineScheduler()

    val topic1 = "user1"
    val schema1 = new Schema()
    schema1.setDriver("mongo")
    schema1.setDb("test1")
    schema1.setTable("user1")
    schema1.setColumns(Map("id" -> "Int", "name" -> "String", "age" -> "String"))

    val sql1 = "select * from user"

    val task1: TaskBean = new TaskBean().initOffline("test_task1", sql, schema, schema)
    task1.setInterval(10)

    offlineScheduler.deploy(task1)

    Thread.sleep(20 * 1000)

    offlineScheduler.cancel(task1.getTaskType()+"_"+task1.getName())

  }
}