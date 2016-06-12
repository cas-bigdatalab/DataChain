package com.github.casbigdatalab.compute.RealTime


import com.github.casbigdatalab.utils.StreamingLogLevels
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, RowFactory, SQLContext}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by duyuanyuan on 2016/6/12.
  */
object Kafka2SparkStreaming {
  /*
  args: 0 数据流的时间间隔
        1 kafka Topic
        2 kafka param
        3 schema
        4 src schema name
        5 Create Table Sql
        6 Execute Sql
   */
  def run(duration : String, topic : String, kafkaParam : String,
          schemaSrc : String, srcName : String, createDecTable : String, execSql : String) {

    StreamingLogLevels.setStreamingLogLevels()

    val conf = new SparkConf().setAppName("RealTime Compute")
//      .setMaster("spark://*:7077")
//      .set("spark.driver.memory", "3g")
//      .set("spark.executor.memory", "10g")
//      .set("spark.cores.max", "24")
//      .set("spark.driver.allowMultipleContexts", "true")
//      .setJars(List("D:\\DataChain\\classes\\artifacts\\datachain_jar\\datachain.jar"))

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(duration.toInt))

    val topics = topic.split(",").map { s =>
      val a = s.split(":")
      (a(0), a(1).toInt)
    }.toMap

    val kafkaParams = kafkaParam.split(";").map { s =>
        val a = s.split("->")
        (a(0), a(1))
      }.toMap

    // Generate the schema based on the string of schema
    val fields : Array[StructField] = Array[StructField]()
    for (fieldName <- schemaSrc.split(" ")) {
      fields.+:(DataTypes.createStructField(fieldName, DataTypes.StringType, true))
    }

    val schema = DataTypes.createStructType(fields)

    val kafkaStream = KafkaUtils.createStream[
      String,
      String,
      StringDecoder,
      StringDecoder
      ](ssc, kafkaParams, topics, StorageLevel.MEMORY_AND_DISK_SER_2)

    val lines = kafkaStream.map(_._2)

    val filedsLength = sc.broadcast(fields.length)

    lines.foreachRDD((rdd: RDD[String], time: Time) => {
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._
      val srcRDD = rdd.map(line => {
        val fields = line.split(",").toSeq
//            for(i <- 0 until filedsLength.value){
//
//            }
        Row(fields)
      })
      // Apply the schema to the RDD.
      val srcDataFrame = sqlContext.createDataFrame(srcRDD, schema)
      srcDataFrame.registerTempTable(srcName)

      sqlContext.sql(createDecTable)
      sqlContext.sql(execSql)

    })

    //启动
    ssc.start()
    ssc.awaitTermination()

  }

  def main(args: Array[String]): Unit = {

    val duration = "1"
    val topics = "user:3"
    val kafkaParam = "metadata.broker.list->*:9092,*:9092,*:9092;group.id->test-consumer-group"
    val schemaSrc = "name age"
    val srcName = "user"
    val createDecTable = """
                           |CREATE TEMPORARY TABLE test
                           |USING org.apache.spark.sql.jdbc
                           |OPTIONS (
                           |  url    'jdbc:mysql://*:3306/test?user=root&password=root',
                           |  dbtable     'user1'
                           |)""".stripMargin
    val execSql = """
                    |INSERT OVERWRITE table test
                    |SELECT * FROM user
                  """.stripMargin
    run(duration, topics, kafkaParam, schemaSrc, srcName, createDecTable, execSql)
  }
}

/** Lazily instantiated singleton instance of SQLContext */
object SQLContextSingleton {

  @transient private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}