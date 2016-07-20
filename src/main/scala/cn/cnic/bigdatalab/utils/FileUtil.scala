package cn.cnic.bigdatalab.utils

import cn.cnic.bigdatalab.task.TaskBean
import cn.cnic.bigdatalab.collection.{AgentSink, AgentChannel, AgentSource, Agent}

import scala.io.Source
import scala.util.parsing.json.JSON

/**
  * Created by xjzhu on 16/7/4.
  */
object FileUtil {


  def fileReader(filePath: String):String={

    var str = ""
    val file = Source.fromFile(filePath)
    val iter = file.buffered
    while (iter.hasNext) {
      val line = iter.head
      str += line
      iter.next()
    }
    file.close()
    str
  }

  def agentReader(agentPath:String): Agent ={
    //val agentPath = Thread.currentThread().getContextClassLoader.getResource("agent.json").getPath
    val agent = Agent.parseJson(fileReader(agentPath))
    agent
  }

  def taskReader(taskPath:String): TaskBean = {
    //val taskPath = Thread.currentThread().getContextClassLoader.getResource("realTimeTask.json").getPath
    val taskBean = TaskBean.parseJson(fileReader(taskPath))
    taskBean
  }



  def main(args: Array[String]): Unit ={

    //schema
    /*val path = Thread.currentThread().getContextClassLoader.getResource("userSchema.json").getPath
    val schema = schemaReader(path)*/

    //agent
    /*val agentPath = Thread.currentThread().getContextClassLoader.getResource("agent.json").getPath
    val agent = Agent.parseJson(fileReader(agentPath))*/

    //task
    val taskPath = Thread.currentThread().getContextClassLoader.getResource("realtime/realTimeTask.json").getPath
    val realtimeTaskBean = TaskBean.parseJson(fileReader(taskPath))

    realtimeTaskBean.getName()

  }




}
