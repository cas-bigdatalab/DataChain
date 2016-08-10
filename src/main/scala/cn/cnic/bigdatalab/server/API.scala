package cn.cnic.bigdatalab.server

import cn.cnic.bigdatalab.common.Quartz
import cn.cnic.bigdatalab.datachain.{Chain, CollectionStep, TaskStep}
import cn.cnic.bigdatalab.task.{OfflineTask, RealTimeTask, SchedulerFactory}
import cn.cnic.bigdatalab.utils.{FileUtil, PropertyUtil}

/**
  * Created by xjzhu@cnic.cn on 2016/8/1.
  */
object API {

  val json_path = PropertyUtil.getPropertyValue("json_path")

  def runRealTimeTask(agentId:String, taskId: String): String ={
    //1.define Collection
    val agent_json_path = json_path + "/" + agentId
    val agent = FileUtil.agentReader(agent_json_path)
    val collectionStep = new CollectionStep().initAgent(agent)

    //2. Define real Task
    val task_json_path = json_path + "/" + "realtime/" + taskId
    val taskBean = FileUtil.taskReader(task_json_path)

    if(Quartz.tasks.contains(taskBean.taskType + "_" + taskBean.name)){
      return "Task exists!"
    }

    val taskStep = new TaskStep().setRealTimeTask(new RealTimeTask(taskBean))

    val chain = new Chain()
    chain.addStep(collectionStep).addStep(taskStep).run()

    "Create OK!"

  }

  def runOfflineTask(taskId: String): String ={

    //1. Define real Task
    val task_json_path = json_path + "/" + "offline/" + taskId
    val taskBean = FileUtil.taskReader(task_json_path)

    if(Quartz.tasks.contains(taskBean.taskType + "_" + taskBean.name)){
      return "Task exists!"
    }

    val taskStep = new TaskStep().setOfflineTask(new OfflineTask(taskBean))

    val chain = new Chain()
    chain.addStep(taskStep).run()
    println(Quartz.tasks)
    "Create OK!"
  }

  def deleteTask(name: String): String = {
    println(Quartz.tasks)
    if(!Quartz.tasks.contains(name)){
      "Task not exist"
    } else{
      val taskType = name.split("_")(0)
      val scheduler = SchedulerFactory(taskType)
      scheduler.cancel(name)
      "Delete OK!"
    }
  }

  def stopTask(name: String): String = {
    println(Quartz.tasks)
    if(!Quartz.tasks.contains(name)){
      "Task not exist"
    } else{
      val taskType = name.split("_")(0)
      val scheduler = SchedulerFactory(taskType)
      scheduler.cancel(name, "STOP")
      "STOP OK!"
    }

  }

  def startTask(name: String): String = {
    println(Quartz.tasks)
    if(!Quartz.tasks.contains(name)){
      "Task not exist"
    } else{
      val taskType = name.split("_")(0)
      val scheduler = SchedulerFactory(taskType)
      scheduler.start(name)
      "START OK!"
    }

  }

}
