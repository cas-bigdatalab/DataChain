package cn.cnic.bigdatalab.task.factory

import cn.cnic.bigdatalab.entity.{Entry, Schema}
import cn.cnic.bigdatalab.task.TaskUtils
import cn.cnic.bigdatalab.utils.PropertyUtil

/**
  * Created by duyuanyuan on 2016/7/25.
  */
class ExternalTask extends TaskBean{
  def initRealtime(name: String, external: String, topic: String, entry: Entry, mapping:String, notificationTopic:String = ""): TaskBean ={
    this.taskType = "realtime"

    //init common params
    init(name, taskType+"_external")

    this.jars = List(external)

    //init app params
    if(notificationTopic.equals("")){
      this.appParams = List(this.taskType+"_"+name, TaskUtils.getDuration(), TaskUtils.getTopic(topic),
        TaskUtils.getKafkaParams(), TaskUtils.wrapDelimiter(entry.mainClass), TaskUtils.wrapDelimiter(entry.menthodName),
        TaskUtils.wrapDelimiter(entry.language), mapping)
    }else{
      this.appParams = List(this.taskType+"_"+name, TaskUtils.getDuration(), TaskUtils.getTopic(topic),
        TaskUtils.getKafkaParams(), TaskUtils.wrapDelimiter(entry.mainClass), TaskUtils.wrapDelimiter(entry.menthodName),
        TaskUtils.wrapDelimiter(entry.language), mapping,
        TaskUtils.getTopic(notificationTopic),
        TaskUtils.getKafkaBrokerList())
    }

    this

  }


  def parseMap(map: Map[String, Any]): ExternalTask ={

    //name
    assert(!map.get("name").get.asInstanceOf[String].isEmpty)
    val name = map.get("name").get.asInstanceOf[String]

    //taskType
    assert(!map.get("taskType").get.asInstanceOf[String].isEmpty)
    val taskType = map.get("taskType").get.asInstanceOf[String]

    //external path
    assert(!map.get("external").get.asInstanceOf[String].isEmpty)
    val external = PropertyUtil.getPropertyValue("external_path") + map.get("external").get.asInstanceOf[String]

    //srcTable
    assert(!map.get("entry").get.asInstanceOf[Map[String, Any]].isEmpty)
    val entry = Entry.parserMap(map.get("entry").get.asInstanceOf[Map[String, Any]])


    taskType match {
      case "realtime" =>{

        //topic for realtime task
        assert(!map.get("topic").get.asInstanceOf[String].isEmpty)
        val topic = map.get("topic").get.asInstanceOf[String]

        //mapping
        assert(!map.get("mapping").get.asInstanceOf[String].isEmpty)
        val mapping = map.get("mapping").get.asInstanceOf[String]

        val notificationTopic = map.getOrElse("notificationTopic", "").asInstanceOf[String]

        initRealtime(name, external, topic, entry, mapping, notificationTopic)

      }
      /*case "offline" =>{

        //sql
        assert(!map.get("sql").get.asInstanceOf[String].isEmpty)
        val sql = map.get("sql").get.asInstanceOf[String]

        //interval
        val interval = map.getOrElse("interval", "-1").asInstanceOf[String]

        initOfflineMultiSchema(name,sql, srcSchemaList:::destSchemaList, interval.toLong)

      }
      case "store" =>{
        //topic for realtime task
        assert(!map.get("topic").get.asInstanceOf[String].isEmpty)
        val topic = map.get("topic").get.asInstanceOf[String]

        //mapping
        assert(!map.get("mapping").get.asInstanceOf[String].isEmpty)
        val mapping = map.get("mapping").get.asInstanceOf[String]

        initStore(name, topic, srcSchemaList(0), destSchemaList(0), mapping)
      }*/
    }

    this

  }

}
