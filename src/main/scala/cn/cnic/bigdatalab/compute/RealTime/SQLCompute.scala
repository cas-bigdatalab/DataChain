package cn.cnic.bigdatalab.compute.realtime

import java.sql.Timestamp
import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date

import akka.io.Udp.SO.Broadcast
import cn.cnic.bigdatalab.compute.HiveSQLContextSingleton
import cn.cnic.bigdatalab.compute.notification.KafkaMessagerProducer
import cn.cnic.bigdatalab.compute.realtime.utils.Utils
import cn.cnic.bigdatalab.transformer.Transformer
import cn.cnic.bigdatalab.utils.StreamingLogLevels
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by duyuanyuan on 2016/6/12.
  */
object SQLCompute{

  val failStatus: String = "Failed"
  val receiveStatus: String = "Received"
  var preMP: Int = _
  val logger:Logger = LoggerFactory.getLogger("SQLCompute")
  val df:DateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");


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
  def run(appName: String, duration : String, topic : String, kafkaParam : String,
          srcName : String, createDecTable : String, execSql : String, mapping:String,
          notificationTopic : String = "", kafkaBrokerList:String = "",
          sqlType: String="", attachSql: String=""){

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
    if(sqlType.equals("hive")){
      preMP = new Date().getHours
    }

    val lines = Kafka2SparkStreaming.getStream(ssc, topic, kafkaParam)

    //var windewIndex = 0
    //var endTime = new Timestamp(System.currentTimeMillis())
    lines.foreachRDD((rdd: RDD[String], time: Time) => {

      val rddCount = rdd.count()
      try{

        /*//window delay
        if(rddCount != 0){
          val first = rdd.filter(_!="").first()
          val row = transformer.transform(first)
          val createTime = row(row.size-1).asInstanceOf[Timestamp]
          val windowdelay:Long = (endTime.getTime - createTime.getTime)/1000 - 1
          logger.error(windewIndex +"-DelayTime: " + windowdelay.toString)
        }*/

        val sqlContext = HiveSQLContextSingleton.getInstance(rdd.sparkContext)

        val srcRDD = rdd.filter(_!="").map(line => {
          //call transformer
          println("line:" + line + "!!!")
          val row = transformer.transform(line)
          println("transformer result: " + row)
          row.toArray.toSeq
        }).filter(_.nonEmpty).map(row => Row.fromSeq(row))

        // Apply the schema to the RDD.
        val srcDataFrame = sqlContext.createDataFrame(srcRDD, schema)
        srcDataFrame.registerTempTable(srcName)

        //Execute SQL tasks
        val tableDescList = createDecTable.split("#-#")
        for( tableDesc <- tableDescList){
          sqlContext.sql(tableDesc)
        }
        if(sqlType.equals("hive")){
          execHiveSql(sqlContext, execSql, attachSql)
        }else{
          sqlContext.sql(execSql)
        }

      }catch {
        case ex: Exception => {
          if(!(notificationTopic.equals("") || kafkaBrokerList.equals(""))){
            val topic = notificationTopic.split(":")(0)
            //val partition = notificationTopic.split(":")(1)
            KafkaMessagerProducer.produce(topic, kafkaBrokerList,failStatus)
          }
          ssc.awaitTerminationOrTimeout(5)
          sc.stop()
        }
      }
      if(!(notificationTopic.equals("") || kafkaBrokerList.equals(""))){
        println("topics：" + notificationTopic)
        val topic = notificationTopic.split(":")(0)
        val partition = notificationTopic.split(":")(1)
        KafkaMessagerProducer.produce(topic, partition, kafkaBrokerList, receiveStatus)
      }
      /*if(rddCount != 0){
        endTime = new Timestamp(System.currentTimeMillis())
        logger.warn(windewIndex +"-EndTime: " + df.format(endTime))
      }
      windewIndex = windewIndex + 1*/
    })

    //启动
    ssc.start()
    ssc.awaitTermination()

  }

  def execHiveSql(sqlContext: HiveContext, execSql: String, attachSql: String): Unit ={
    val mp: Int = new Date().getHours
    sqlContext.sql(execSql.replace("%mp%", mp.toString))
    if(preMP != mp){
      val attachSqlList = attachSql.split("#-#")
      for(as <- attachSqlList){
        sqlContext.sql(as.replace("preMP", preMP.toString))
      }
      preMP = mp
    }

  }

  def main(args: Array[String]): Unit = {

    //mysql test
    /*val createDecTable = """
                           |CREATE TEMPORARY TABLE mysql_test
                           |USING org.apache.spark.sql.jdbc
                           |OPTIONS (
                           |  url    'jdbc:mysql://10.0.71.1:3306/spark?user=root&password=root',
                           |  dbtable     'test'
                           |)""".stripMargin*/
// mongodb test
/*    val createDecTable = """
                           |CREATE TEMPORARY TABLE test(
                           | name String, age Int
                           |)USING com.stratio.datasource.mongodb
                           |OPTIONS (
                           |  host '*:27017',
                           |  database 'test',
                           |  collection 'age'
                           |)""".stripMargin*/

    //hive test
//    val createDecTable = """
//                           |CREATE TABLE IF NOT EXISTS user(
//                           |name STRING, age INT
//                           |)""".stripMargin

//    //Hbase test
//    val createDecTable = """
//                           |CREATE TABLE IF NOT EXISTS hbase_table(key int, name string, age int)
//                           |STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
//                           |WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf1:name,cf2:age")
//                           |TBLPROPERTIES ("hbase.table.name" = "user", "hbase.mapred.output.outputtable" = "user")""".stripMargin

//    val createDecTable =
//      """
//        |CREATE TEMPORARY TABLE test
//        |USING solr
//        |OPTIONS (
//        |  zkhost    '10.0.71.14:2181,10.0.71.17:2181,10.0.71.38:2181',
//        |  collection     'user1',
//        |  soft_commit_secs '5'
//        |)""".stripMargin
//    val execSql = """
//                    |INSERT INTO table mysql_test
//                    |SELECT * FROM streaming_test
//                  """.stripMargin
//    val appName = "mysql test"
//    val duration = "1"
//    val topics = "test :1"
//    val kafkaParam = "zookeeper.connect->10.0.71.20:2181,10.0.71.26:2181,10.0.71.27:2181;group.id->test-consumer-group"
//    val schemaSrc = "id:Int,name :String,age:Int"
//    val srcName = "streaming_test"
//    val mapping = "/opt/csvMapping_user.json"
//    val notificationTopic = "notification:1"
//    val kafkaBrokerList = "10.0.71.20:9092,10.0.71.26:9092,10.0.71.27:9092"
//    val sqlType = "hive"
//    val attachSql =
//      """INSERT INTO TABLE test_partition_bucket PARTITION (mp=24, age)
//        |SELECT id, name, age FROM test_partition_bucket WHERE mp=preMP
//        |#-#
//        |TRUNCATE TABLE test_partition_bucket PARTITION (mp=preMP)""".stripMargin
//    val createDecTable = "CREATE TABLE IF NOT EXISTS test_partition_bucket( id Int, name String ) partitioned by( mp int, age int)"
//    val execSql = "insert into table test_partition_bucket partition(mp=$mp, age) select * from streaming_test"

    val appName = args(0)
    val duration = args(1)
    val topics = args(2)
    val kafkaParam = args(3)
    val srcName = args(4)
    val createDecTable = args(5)
    val execSql = args(6)
    val mapping = args(7)
    var notificationTopic = ""
    var kafkaBrokerList = ""
    var sqlType = ""
    var attachSql = ""
    if (args.size == 10){
      notificationTopic = args(8)
      kafkaBrokerList = args(9)
    }
    if(args.size == 12){
      notificationTopic = args(8)
      kafkaBrokerList = args(9)
      sqlType = args(10)
      attachSql = args(11)
    }

    run(appName, duration, topics, kafkaParam, srcName, createDecTable, execSql, mapping, notificationTopic, kafkaBrokerList, sqlType, attachSql)
  }
}