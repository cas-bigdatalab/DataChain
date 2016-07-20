package cn.cnic.bigdatalab.compute.realtime

import cn.cnic.bigdatalab.compute.SQLContextSingleton
import cn.cnic.bigdatalab.utils.StreamingLogLevels
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Flora on 2016/5/25.
  */
object SparkMySql {
  case class Person(id:Int, name:String, address:String, company:String, age:Int, workno:Int)
  def main(agrs: Array[String]): Unit = {

    StreamingLogLevels.setStreamingLogLevels()

    val sparkConf = new SparkConf().setAppName("spark sql test")
      .setMaster("spark://*:7077")
      .set("spark.driver.memory", "3g")
      .set("spark.executor.memory", "10g")
      .set("spark.cores.max", "24")
      .set("spark.driver.allowMultipleContexts", "true")
      .setJars(List("D:\\DataChain\\classes\\artifacts\\datachain_jar\\datachain.jar"))

    val sparkContext = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sparkContext, Seconds(1))

    //    org.apache.log4j.Logger.getRootLogger().setLevel(new org.apache.log4j.Level(2147483647, "OFF", 0))
    val topic = "test3"
    val topicSet = topic.split(" ").toSet

    //create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "*", "group.id" -> "test-consumer-group")

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicSet
    )

    val lines = messages.map(_._2)

    lines.foreachRDD((rdd: RDD[String], time: Time) => {
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._

      val personDataFrame = rdd.map(line => {
        val data = line.split(",")
        Person(data(0).toInt, data(1), data(2), data(3), data(4).toInt, data(5).toInt)
      }).toDF()

      personDataFrame.registerTempTable("person")

//      val user = sqlContext.jdbc(JDBCURL, "user")

//      val user = rdd.sparkContext.textFile("D://user.txt").map(_.split(",")).map(data => Person(data(0).toInt, data(1), data(2), data(3), data(4).toInt, data(5).toInt)).toDF()

//      user.registerTempTable("user")

//      sqlContext.sql("CREATE TABLE IF NOT EXISTS user1 (key INT, value STRING)" +
//        "USING com.mysql.jdbc.Driver")

      sqlContext.sql("""
                       |CREATE TEMPORARY TABLE test
                       |USING org.apache.spark.sql.jdbc
                       |OPTIONS (
                       |  url    'jdbc:mysql://*:3306/test?user=root&password=root',
                       |  dbtable     'user1'
                       |)""".stripMargin)



      sqlContext.sql("insert into table test select name, age from person")

    })

    //启动
    ssc.start()
    ssc.awaitTermination()

  }

}
