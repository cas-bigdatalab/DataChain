package cn.cnic.bigdatalab.compute.realtime

import cn.cnic.bigdatalab.compute.HiveSQLContextSingleton
import cn.cnic.bigdatalab.transformer.Transformer
import cn.cnic.bigdatalab.utils.StreamingLogLevels
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by duyuanyuan on 2016/6/12.
  */
object SQLCompute {

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
          srcName : String, createDecTable : String, execSql : String, mapping:String) {

    StreamingLogLevels.setStreamingLogLevels()

    val conf = new SparkConf().setAppName(appName)
      .setMaster("spark://10.0.71.1:7077")
      .set("spark.driver.memory", "3g")
      .set("spark.executor.memory", "10g")
      .set("spark.cores.max", "12")
      .set("spark.driver.allowMultipleContexts", "true")
      .setJars(List("D:\\git\\DataChain\\out\\artifacts\\datachain_jar\\datachain.jar"))

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(duration.toInt))

    // Create transformer
    @transient val transformer = new Transformer(mapping)
    val schema = Utils.getSchema(transformer)

    val lines = Kafka2SparkStreaming.createStream(ssc, topic, kafkaParam)

    lines.foreachRDD((rdd: RDD[String], time: Time) => {
      val sqlContext = HiveSQLContextSingleton.getInstance(rdd.sparkContext)

      //Get Row RDD
      val srcRDD = rdd.filter(_!="").map(line => {
        //call transformer
        val row = transformer.transform(line)
        Row.fromSeq(row.toArray.toSeq)
      })

      // Apply the schema to the RDD.
      val srcDataFrame = sqlContext.createDataFrame(srcRDD, schema)
      srcDataFrame.registerTempTable(srcName)

     //Execute SQL tasks
     val tableDescList = createDecTable.split("#-#")
      for( tableDesc <- tableDescList){
        sqlContext.sql(tableDesc)
      }
      sqlContext.sql(execSql)

    })

    //启动
    ssc.start()
    ssc.awaitTermination()

  }

  def main(args: Array[String]): Unit = {

    //mysql test
//    val createDecTable = """
//                           |CREATE TEMPORARY TABLE user
//                           |USING org.apache.spark.sql.jdbc
//                           |OPTIONS (
//                           |  url    'jdbc:mysql://10.0.50.216:3306/spark?user=root&password=root',
//                           |  dbtable     'user'
//                           |)""".stripMargin
// mongodb test
//    val createDecTable = """
//                           |CREATE TEMPORARY TABLE test(
//                           | name String, age Int
//                           |)USING com.stratio.datasource.mongodb
//                           |OPTIONS (
//                           |  host '*:27017',
//                           |  database 'test',
//                           |  collection 'age'
//                           |)""".stripMargin

    //hive test
    val createDecTable = """
                           |CREATE TABLE IF NOT EXISTS user(
                           |name STRING, age INT
                           |)""".stripMargin

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
    val execSql = """
                    |INSERT INTO table user
                    |SELECT name, age FROM test
                  """.stripMargin
    val appName = "transformer test"
    val duration = "1"
    val topics = "test :1"
    val kafkaParam = "zookeeper.connect->10.0.71.20:2181,10.0.71.26:2181,10.0.71.27:2181;group.id->test-consumer-group"
    val schemaSrc = "id:Int,name :String,age:Int"
    val srcName = "test"
    val mapping = "D:\\git\\DataChain\\conf\\csvMapping_user.json"

//    val appName = args(0)
//    val duration = args(1)
//    val topics = args(2)
//    val kafkaParam = args(3)
//    val srcName = args(4)
//    val createDecTable = args(5)
//    val execSql = args(6)
//    val mapping = args(7)

    println("create dec table sql:" + createDecTable)
    println("exec sql:" + execSql)

    run(appName, duration, topics, kafkaParam, srcName, createDecTable, execSql, mapping)
  }
}