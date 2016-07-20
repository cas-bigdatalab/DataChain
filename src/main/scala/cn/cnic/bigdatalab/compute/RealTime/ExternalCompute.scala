package cn.cnic.bigdatalab.compute.realtime

import cn.cnic.bigdatalab.compute.HiveSQLContextSingleton
import cn.cnic.bigdatalab.transformer.Transformer
import cn.cnic.bigdatalab.utils.{FileUtil, StreamingLogLevels}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Flora on 2016/7/20.
  */
object ExternalCompute {
  /*
  args: 0 app name
        1 数据流的时间间隔
        2 kafka Topic
        3 kafka param
        4 src schema name
        5 Create Table Sql
        6 Execute Sql
        7 transformer

   */
  def run(appName: String, duration : String, topic : String, kafkaParam : String, mainClass: String,
          externalPath : String, filelist: String, mapping:String) {

    StreamingLogLevels.setStreamingLogLevels()

    val conf = new SparkConf().setAppName(appName)
//      .setMaster("spark://10.0.71.1:7077")
//      .set("spark.driver.memory", "3g")
//      .set("spark.executor.memory", "10g")
//      .set("spark.cores.max", "12")
//      .set("spark.driver.allowMultipleContexts", "true")
//      .setJars(List("D:\\git\\DataChain\\out\\artifacts\\datachain_jar\\datachain.jar"))

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(duration.toInt))

    // Create transformer
    @transient val transformer = new Transformer(mapping)
    val schema = Utils.getSchema(transformer)

    //Runtime compile
    val interpreter = InterpreterSingleton.getInstance()
    for(file <- filelist.split(",")){
      interpreter.compileString(FileUtil.fileReader(externalPath + "\\" + file))
    }

    val lines = Kafka2SparkStreaming.createStream(ssc, topic, kafkaParam)

    lines.foreachRDD((rdd: RDD[String], time: Time) => {
      val srcRDD = rdd.filter(_!="").map(line => {
        //call transformer
//        val row = transformer.transform(line)
//        Row.fromSeq(row.toArray.toSeq)
        interpreter.interpret(s"""new $mainClass().process("$schema", "$line")""")
      })
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
//    val externalPath = "D:\\git\\DataChain\\externalcode"
//    val filelist = "ConvertColumn.scala,ConnectMySql.scala"
//    val mainClass = "ConnectMySql"

        val appName = args(0)
        val duration = args(1)
        val topics = args(2)
        val kafkaParam = args(3)
        val externalPath = args(4)
        val filelist = args(5)
        val mainClass = args(6)
        val mapping = args(7)

    run(appName, duration, topics, kafkaParam, mainClass, externalPath, filelist, mapping)
  }

}
