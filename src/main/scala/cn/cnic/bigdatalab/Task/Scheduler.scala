package cn.cnic.bigdatalab.Task

import cn.cnic.bigdatalab.utils.PropertyUtil

import scala.sys.process.Process

/**
  * Created by Flora on 2016/6/23.
  */
class Scheduler {
//  private val queue: List[BaseTask] = _

  def deploy(taskParams: List[String], appParams: Map[String, String], sparkParams: Map[String, String]): Unit ={

    val command: StringBuffer = new StringBuffer()
    command.append("cd ").append(PropertyUtil.getPropertyValue("spark_home")).append(";")
      .append("./bin/spark-submit ").append("--class ").append(appParams.get("class"))
      .append(" --master ").append(sparkParams.get("master"))
      .append(" --executor-memory ").append(sparkParams.get("executor-memory"))
      .append(" --total-executor-cores ").append(sparkParams.get("total-executor-cores"))
      .append(" ").append(appParams.get("path"))

    command.append(taskParams.mkString(" ", " ",""))

    val deployCmd =  "ssh root@" + PropertyUtil.getPropertyValue("spark_host") + " /bin/bash " + command

    Process(Seq("bash","-c", deployCmd)).!

  }

}
