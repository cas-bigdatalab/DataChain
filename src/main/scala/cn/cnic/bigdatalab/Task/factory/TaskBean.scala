package cn.cnic.bigdatalab.task.factory

import cn.cnic.bigdatalab.utils.PropertyUtil

import scala.util.parsing.json.JSON

/**
  * Created by duyuanyuan on 2016/7/25.
  */
trait TaskBean{
  var name: String = _
  var taskType: String = _
  var priority: Int = _
//  var interval: Long = _
  var notificationTopic: String = _
  var expression: String = _
  var appParams: List[String] = _
  var taskParams: Map[String, String] = _
  var sparkParams: Map[String, String] = _
  var jars: List[String] = _

  def parseMap(map: Map[String, Any]): TaskBean

  def init(name: String, taskType: String): Unit ={
    this.name = name
    ///init task params
    this.taskParams = createTaskParams(taskType)

    //init spark params
    this.sparkParams = createSparkParams()
  }



  protected def createTaskParams(tType: String): Map[String, String] = {
    Map("class" -> PropertyUtil.getPropertyValue(tType + "_class"),
      "path" -> PropertyUtil.getPropertyValue("datachain_path"))
  }

  protected def createSparkParams(): Map[String, String] = {
    Map("master" -> PropertyUtil.getPropertyValue("master"), "executor-memory" -> PropertyUtil.getPropertyValue("executor-memory"),
      "total-executor-cores" -> PropertyUtil.getPropertyValue("total-executor-cores"))
  }
}
