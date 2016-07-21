package cn.cnic.bigdatalab.compute.realtime

import cn.cnic.bigdatalab.transformer.Transformer
import cn.cnic.bigdatalab.utils.{FileUtil, StreamingLogLevels}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by Flora on 2016/7/20.
  * 运行时编译用户源码，实现对实时采集数据的动态处理
  * 具体实现步骤：
  * 1、用户上传源码或界面提供编辑器
  *    （1）定义process(schema:String, line:String)方法，自定义处理逻辑
  *    （2）给定主类和类的继承关系
  * 2、将用户源码文件分发到spark集群的相同目录下
  * 3、在spark集群上启动RunTimeCompute任务
  */
object RunTimeCompute {
  /*
  args: 0 app name
        1 数据流的时间间隔
        2 kafka Topic
        3 kafka param
        4 运行时执行主类
        5 运行时路径
        6 需要运行时编译的文件列表
        7 transformer

   */
  def run(appName: String, duration : String, topic : String, kafkaParam : String, mainClass: String,
          runtimePath : String, fileList: String, mapping:String) {

    StreamingLogLevels.setStreamingLogLevels()

    val conf = new SparkConf().setAppName(appName)
//      .setMaster("spark://10.0.71.1:7077")
//      .set("spark.driver.memory", "3g")
//      .set("spark.executor.memory", "4g")
//      .set("spark.cores.max", "8")
//      .set("spark.driver.allowMultipleContexts", "true")
//      .setJars(List("D:\\git\\DataChain\\out\\artifacts\\datachain_jar\\datachain.jar"))

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(duration.toInt))

    // Create transformer
    @transient val transformer = new Transformer(mapping)
    val schema = transformer.getSchema().mkString("",",","")

    val lines = Kafka2SparkStreaming.getStream(ssc, topic, kafkaParam)

    lines.foreachRDD((rdd: RDD[String], time: Time) => {

      rdd.filter(_!="").map(line => {

        //call transformer
//        val row = transformer.transform(line)
//        Row.fromSeq(row.toArray.toSeq)
        val interpreter = InterpreterSingleton.getInstance()
        for(file <- fileList.split(",")){
          interpreter.compileString(FileUtil.fileReader(runtimePath + "/" + file))
        }
        interpreter.interpret(s"""new $mainClass().process("$schema", "$line")""")
      }).count()

    })

    //启动
    ssc.start()
    ssc.awaitTermination()

  }

  def main(args: Array[String]): Unit = {
//    val appName = "External test"
//    val duration = "1"
//    val topics = "test :1"
//    val kafkaParam = "zookeeper.connect->10.0.71.20:2181,10.0.71.26:2181,10.0.71.27:2181;group.id->test-consumer-group"
//    val mapping = "D:\\git\\DataChain\\conf\\csvMapping_user.json"
//    val runtimePath = "/opt/datachain/runtime"
//    val filelist = "ConvertColumn.scala,ConnectMySql.scala"
//    val mainClass = "ConnectMySql"

        val appName = args(0)
        val duration = args(1)
        val topics = args(2)
        val kafkaParam = args(3)
        val runtimePath = args(4)
        val fileList = args(5)
        val mainClass = args(6)
        val mapping = args(7)

    run(appName, duration, topics, kafkaParam, mainClass, runtimePath, fileList, mapping)
  }

}
