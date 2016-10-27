package cn.cnic.bigdatalab.compute.offline

import java.util

import cn.cnic.bigdatalab.compute.HiveSQLContextSingleton
import cn.cnic.bigdatalab.compute.notification.KafkaMessagerProducer
import cn.cnic.bigdatalab.compute.realtime.Kafka2SparkStreaming
import cn.cnic.bigdatalab.compute.realtime.utils.Utils
import cn.cnic.bigdatalab.transformer.Transformer
import cn.cnic.bigdatalab.utils.{PropertyUtil, StreamingLogLevels}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xjzhu@cnic.cn on 2016/10/19.
  */
object ExternalOffline {
  val failStatus: String = "Failed"
  val receiveStatus: String = "Received"

  def run(appName: String, mainClass : String, methodName: String,language: String,
          parameterMap:Map[String,String],
          notificationTopic : String = "", kafkaBrokerList:String = "") {

    try{
      //get spark context
      val conf = new SparkConf().setAppName(appName)
      val sc = new SparkContext(conf)

      language.toLowerCase match {
        case "scala" => {
          Utils.invoker(mainClass+"$", methodName, sc, parameterMap)
        }
        case "java" => {
          Utils.invokeStaticMethod(mainClass, methodName, sc, parameterMap)
        }
      }

    }catch {
      case runtime: RuntimeException => {
        if(!(notificationTopic.equals("") || kafkaBrokerList.equals(""))){
          val topic = notificationTopic.split(":")(0)
          //val partition = notificationTopic.split(":")(1)
          KafkaMessagerProducer.produce(topic, kafkaBrokerList,failStatus)
        }
      }

    }

    if(!(notificationTopic.equals("") || kafkaBrokerList.equals(""))){
      val topic = notificationTopic.split(":")(0)
      val partition = notificationTopic.split(":")(1)
      KafkaMessagerProducer.produce(topic, partition, kafkaBrokerList, receiveStatus)

    }
  }

  def main(args: Array[String]): Unit = {
    val appName = args(0)
    val mainClass = args(1)
    val methodName = args(2)
    val language = args(3)
    val parameters = args(4)
    val parametersList = parameters.split(PropertyUtil.getPropertyValue("create_sql_separator"))
    var parametersMap:Map[String,String] = Map()
    for( param <- parametersList){
      val pair = param.split(":")
      val key = pair(0)
      val value = pair(1)
      parametersMap += (key->value)
    }

    var notificationTopic = ""
    var kafkaBrokerList = ""
    if (args.size == 7){
      notificationTopic = args(5)
      kafkaBrokerList = args(6)
    }

    run(appName, mainClass, methodName, language, parametersMap, notificationTopic, kafkaBrokerList)
  }
}
