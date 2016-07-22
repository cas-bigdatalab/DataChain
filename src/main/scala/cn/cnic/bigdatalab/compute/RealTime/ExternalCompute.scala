package cn.cnic.bigdatalab.compute.realtime

import cn.cnic.bigdatalab.compute.realtime.utils.Utils
import cn.cnic.bigdatalab.transformer.Transformer
import cn.cnic.bigdatalab.utils.{FileUtil, PropertyUtil, StreamingLogLevels}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}
import org.restlet.data.Language

/**
  * Created by Flora on 2016/7/22.
  */
object ExternalCompute {
  /*
  args: 0 app name
        1 数据流的时间间隔
        2 kafka Topic
        3 kafka param
        4 运行时执行主类
        5
        5 运行时路径
        6 需要运行时编译的文件列表
        7 transformer

   */
  def run(appName: String, duration : String, topic : String, kafkaParam : String, mainClass : String, argsLength: String,
          jarPath : String, language: String, mapping:String) {

    StreamingLogLevels.setStreamingLogLevels()

    val conf = new SparkConf().setAppName(appName)
          .setMaster("spark://10.0.71.1:7077")
          .set("spark.driver.memory", "3g")
          .set("spark.executor.memory", "10g")
          .set("spark.cores.max", "12")
          .set("spark.driver.allowMultipleContexts", "true")
          .setJars(List("D:\\git\\DataChain\\out\\artifacts\\datachain_jar\\datachain.jar", jarPath))

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(duration.toInt))

    // Create transformer
    @transient val transformer = new Transformer(mapping)
    val schema = transformer.getSchema().mkString("",",","")
    val methodName = PropertyUtil.getPropertyValue("sdk_method")

    val lines = Kafka2SparkStreaming.getStream(ssc, topic, kafkaParam)

    lines.foreachRDD((rdd: RDD[String], time: Time) => {
      argsLength match {
        case "1" =>{
          language match {
            case "scala" => {
              val proc = Class.forName(mainClass).newInstance.asInstanceOf[{ def process(rdd: RDD[String]): Unit }]
              proc.process(rdd)
            }
            case "java" => {
              Utils.invokeStaticMethod(mainClass, methodName, rdd)
            }
          }
        }
        case "2" =>{
          rdd.filter(_!="").map(line => {
            //call transformer
            //        val row = transformer.transform(line)
            //        Row.fromSeq(row.toArray.toSeq)
            language match {
              case "scala" => {
                val proc = Class.forName(mainClass).newInstance.asInstanceOf[{ def process(schema: String, line: String): Unit }]
                proc.process(schema, line)
              }
              case "java" => {
                Utils.invokeStaticMethod(mainClass, methodName, schema, line)
              }
            }
          }).count()
        }
        case _ => throw new IllegalArgumentException("args length  error")
      }

    })

    //启动
    ssc.start()
    ssc.awaitTermination()

  }

  def main(args: Array[String]): Unit = {
    val appName = "External test"
    val duration = "1"
    val topics = "test :1"
    val kafkaParam = "zookeeper.connect->10.0.71.20:2181,10.0.71.26:2181,10.0.71.27:2181;group.id->test-consumer-group"
    val mapping = "D:\\git\\DataChain\\conf\\csvMapping_user.json"
    val jarPath = "D:\\git\\DataChain\\external\\TestJava.jar"
    val mainClass = "cnic.bigdata.external.TestMysql"
    val argsLength = "2"
    val language = "java"

//    val appName = args(0)
//    val duration = args(1)
//    val topics = args(2)
//    val kafkaParam = args(3)
//    val runtimePath = args(4)
//    val fileList = args(5)
//    val mainClass = args(6)
//    val mapping = args(7)

    run(appName, duration, topics, kafkaParam, mainClass, argsLength, jarPath, language, mapping)
  }

}
