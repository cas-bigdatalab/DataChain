package cn.cnic.bigdatalab.task

import akka.actor._
import akka.actor.Props
import akka.util.Timeout

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import akka.actor.ActorSystem
import akka.japi.Option.Some
import cn.cnic.bigdatalab.common.Quartz
import cn.cnic.bigdatalab.task.factory.{SQLTask, TaskBean, TaskTypeFactory}
import cn.cnic.bigdatalab.entity.Schema
import cn.cnic.bigdatalab.utils.{PropertyUtil, SshUtil}
import com.typesafe.akka.extension.quartz.QuartzSchedulerExtension

import scala.sys.process.Process

/**
  * Created by Flora on 2016/6/23.
  */

trait Scheduler{
  def deploy(taskInstance: TaskBean)

  def cancel(name: String)

  def stop(name: String)

  def execute(taskInstance: TaskBean): Unit ={
    val command: StringBuffer = new StringBuffer()
    command.append("cd ").append(PropertyUtil.getPropertyValue("spark_home")).append(";")
    command.append("./bin/spark-submit ").append("--class ").append(taskInstance.taskParams.get("class").get)
      .append(" --master ").append(taskInstance.sparkParams.get("master").get)
      .append(" --executor-memory ").append(taskInstance.sparkParams.get("executor-memory").get)
      .append(" --total-executor-cores ").append(taskInstance.sparkParams.get("total-executor-cores").get)

    if(taskInstance.jars != null && !taskInstance.jars.isEmpty){
      command.append(" --jars ").append(taskInstance.jars.mkString("", ",",""))
    }

    command.append(" ").append(taskInstance.taskParams.get("path").get)
      .append(taskInstance.appParams.mkString(" ", " ",""))

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
    val name = taskInstance.taskType+"_"+taskInstance.name
    Quartz.tasks += (name -> name)
  }

  override def cancel(name: String): Unit = {
    val killCmd = "ps -ef | grep "+ Quartz.tasks.getOrElse(name, "") + "| grep -v grep | awk '{print $2}' | xargs kill "
    SshUtil.exec(killCmd, PropertyUtil.getPropertyValue("spark_host"), PropertyUtil.getPropertyValue("spark_host_user"),
      PropertyUtil.getPropertyValue("spark_host_password"))
    Quartz.tasks.remove(name)
  }

  override def stop(name: String): Unit = {
    val killCmd = "ps -ef | grep "+ Quartz.tasks.getOrElse(name, "") + "| grep -v grep | awk '{print $2}' | xargs kill "
    SshUtil.exec(killCmd, PropertyUtil.getPropertyValue("spark_host"), PropertyUtil.getPropertyValue("spark_host_user"),
      PropertyUtil.getPropertyValue("spark_host_password"))
  }
}

class OfflineActor(taskInstance: TaskBean) extends Actor{

  def receive = {
    case task: OfflineScheduler => task.execute(taskInstance)
    case _ => ()
  }
}

class OfflineScheduler extends Scheduler{


  override def deploy(taskInstance: TaskBean): Unit ={
    import scala.concurrent.ExecutionContext.Implicits.global
    val act = Quartz.system.actorOf(Props(new OfflineActor(taskInstance)), taskInstance.taskType+"_"+taskInstance.name)
    implicit val time = Timeout(5 seconds)

    val expression = taskInstance.expression


    if(expression != ""){
      val name = taskInstance.taskType+"_"+taskInstance.name
      Quartz.qse.createSchedule(name, Some(name), expression, None)
      Quartz.qse.schedule(name, act, this)

      Quartz.tasks += (name -> taskInstance)
      return
    }


//    val interval = taskInstance.interval
////    val cancellable = system.scheduler.schedule(0 milliseconds,interval seconds, act, this)
////    val cancellable = system.scheduler.scheduleOnce(0 milliseconds, act, this)
//    val cancellable = interval match {
//      case -1 => Quartz.system.scheduler.scheduleOnce(0 milliseconds, act, this)
//      case x => Quartz.system.scheduler.schedule(0 milliseconds,x seconds, act, this)
//    }
//    Quartz.tasks += (taskInstance.taskType+"_"+taskInstance.name -> cancellable)


  }

  override def cancel(name: String): Unit ={
    if(Quartz.tasks.get(name).isEmpty){
      return
    }
    if(Quartz.tasks.get(name).get.isInstanceOf[String]){
      Quartz.qse.cancelJob(Quartz.tasks.get(name).get.asInstanceOf[String])
    }
    if(Quartz.tasks.get(name).get.isInstanceOf[Cancellable]){
      Quartz.tasks.get(name).get.asInstanceOf[Cancellable].cancel()
    }
    Quartz.tasks.remove(name)

  }

  override def stop(name: String): Unit = {
    if(Quartz.tasks.get(name).isEmpty){
      return
    }
    if(Quartz.tasks.get(name).get.isInstanceOf[String]){
      Quartz.qse.cancelJob(Quartz.tasks.get(name).get.asInstanceOf[String])
    }
    if(Quartz.tasks.get(name).get.isInstanceOf[Cancellable]){
      Quartz.tasks.get(name).get.asInstanceOf[Cancellable].cancel()
    }
  }
}

object SchedulerFactory{
  def apply(kind: String) = kind match {
    case "offline" => new OfflineScheduler
    case "realtime" => new RealTimeScheduler
    case _ => throw new IllegalArgumentException("Type error")
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
    schema.setColumns(ArrayBuffer("id:Int","name:String", "age:String"))
    //schema.setColumns(Map("id" -> "Int", "name" -> "String", "age" -> "String"))

    val sql = "select * from user"

    val task: TaskBean = new SQLTask().initRealtime("test_task", sql, topic, schema, schema, "mapping")

    scheduler.deploy(task)

    val offlineScheduler = new OfflineScheduler()

    val topic1 = "user1"
    val schema1 = new Schema()
    schema1.setDriver("mongo")
    schema1.setDb("test1")
    schema1.setTable("user1")
    schema1.setColumns(ArrayBuffer("id:Int","name:String", "age:String"))

    val sql1 = "select * from user"

    val task1: TaskBean = new SQLTask().initOffline("test_task1", sql, schema, schema)

    offlineScheduler.deploy(task1)

    Thread.sleep(20 * 1000)

    offlineScheduler.cancel(task1.taskType+"_"+task1.name)

  }
}