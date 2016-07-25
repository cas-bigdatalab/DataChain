package cn.cnic.bigdatalab.task.factory

import scala.util.parsing.json.JSON


object TaskFactory{
  def apply(kind: String) = kind match {
    case "sql" => new SQLTask
    case "external" => new ExternalTask

  }

}

object Json2Task{
  def parseJson(jsonStr: String) : TaskBean = {

    val mapping = JSON.parseFull(jsonStr).get
    val map: Map[String, Any] = mapping.asInstanceOf[Map[String, Any]].get("Task").get.asInstanceOf[Map[String, Any]]

    var task : TaskBean = null

    if(!map.get("sql").getOrElse("").asInstanceOf[String].isEmpty){
      task = TaskFactory("sql")
    }else if(!map.get("external").getOrElse("").asInstanceOf[String].isEmpty){
      task = TaskFactory("external")
    }
    task.parseMap(map)

  }
}
