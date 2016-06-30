package cn.cnic.bigdatalab.compute.RealTime

import cn.cnic.bigdatalab.utils.{FieldTypeUtil, StreamingLogLevels}
import com.github.casbigdatalab.datachain.transformer.Transformer
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{DataTypes, StructField}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer


/**
  * Created by duyuanyuan on 2016/6/12.
  */
object Kafka2SparkStreaming {

  def getSqlContext(sqlType : String, sparkContext : SparkContext): SQLContext = sqlType.toLowerCase() match {
    case "hive" => HiveSQLContextSingleton.getInstance(sparkContext)
    case _ => SQLContextSingleton.getInstance(sparkContext)
  }

  /*
  args: 0 数据流的时间间隔
        1 kafka Topic
        2 kafka param
        3 schema
        4 src schema name
        5 Create Table Sql
        6 Execute Sql
        7 transformer
        8 sql type

   */
  def run(duration : String, topic : String, kafkaParam : String,
          schemaSrc : String, srcName : String, createDecTable : String, execSql : String, mapping:String, sqlType: String="") {

    StreamingLogLevels.setStreamingLogLevels()

    val conf = new SparkConf().setAppName("RealTime Compute")
//      .setMaster("spark://10.0.50.216:7077")
//      .set("spark.driver.memory", "3g")
//      .set("spark.executor.memory", "10g")
//      .set("spark.cores.max", "12")
//      .set("spark.driver.allowMultipleContexts", "true")
//      .setJars(List("D:\\DataChain\\classes\\artifacts\\datachain_jar\\datachain.jar"))

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(duration.toInt))

    val topics = topic.replaceAll(" ", "").split(",").map { s =>
      val a = s.split(":")
      (a(0), a(1).toInt)
    }.toMap

    val kafkaParams = kafkaParam.replaceAll(" ", "").split(";").map { s =>
        val a = s.split("->")
        (a(0), a(1))
      }.toMap

    // Generate the schema based on the string of schema
    var fields : Array[StructField] = Array[StructField]()
    val schemas = schemaSrc.replaceAll(" ", "").split(",")
    for (field <- schemas) {
      val Array(fieldName, fieldType) = field.split(":")
      fields = DataTypes.createStructField(fieldName, FieldTypeUtil.stringToDataType(fieldType), true) +: fields
    }
    val schema = DataTypes.createStructType(fields.reverse)

    //Create Kafka Stream and currently only support kafka with String messages
    val kafkaStream = KafkaUtils.createStream[
      String,
      String,
      StringDecoder,
      StringDecoder
      ](ssc, kafkaParams, topics, StorageLevel.MEMORY_AND_DISK_SER_2)

    //Create transformer
//    val transfomer = new Transformer(mapping)

    //Get the messages and execute operations
    val lines = kafkaStream.map(_._2)
    lines.foreachRDD((rdd: RDD[String], time: Time) => {
      val sqlContext = getSqlContext(sqlType, rdd.sparkContext)

      //Get Row RDD
      val srcRDD = rdd.map(line => {
        //call transformer
        //val row = transfomer.transform(line)

        val fields = line.split(",")
        var row = new ArrayBuffer[Any]()
        for(i <- 0 until fields.length){
          val value = FieldTypeUtil.parseDataType(schemas(i).split(":")(1), fields(i).trim)
          row += value
        }
        Row.fromSeq(row.toArray.toSeq)
      })

      // Apply the schema to the RDD.
      val srcDataFrame = sqlContext.createDataFrame(srcRDD, schema)
      srcDataFrame.registerTempTable(srcName)

     //Execute SQL tasks
      sqlContext.sql(createDecTable)
      sqlContext.sql(execSql)

    })

    //启动
    ssc.start()
    ssc.awaitTermination()

  }

  def main(args: Array[String]): Unit = {

    //mysql test
//    val createDecTable = """
//                           |CREATE TEMPORARY TABLE test
//                           |USING org.apache.spark.sql.jdbc
//                           |OPTIONS (
//                           |  url    'jdbc:mysql://10.0.71.7:3306/test?user=root&password=root',
//                           |  dbtable     'user1'
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
//    val createDecTable = """
//                           |CREATE TABLE IF NOT EXISTS test(
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
//        |  zkhost    'jdbc:mysql://10.0.71.7:3306/test?user=root&password=root',
//        |  collection     'user1',
//        |  soft_commit_secs
//        |)""".stripMargin
//    val execSql = """
//                    |INSERT INTO table test
//                    |SELECT name, age FROM user
//                  """.stripMargin

    val duration = args(0)
    val topics = args(1)
    val kafkaParam = args(2)
    val schemaSrc = args(3)
    val srcName = args(4)
    val createDecTable = args(5)
    val execSql = args(6)
    val mapping = args(7)
    val sqlType = args(8)

    println("create dec table sql:" + createDecTable)
    println("exec sql:" + createDecTable)

    run(duration, topics, kafkaParam, schemaSrc, srcName, createDecTable, execSql, mapping, sqlType)
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

/** Lazily instantiated singleton instance of SQLContext */
object HiveSQLContextSingleton {

  @transient private var instance: HiveContext = _

  def getInstance(sparkContext: SparkContext): HiveContext = {
    if (instance == null) {
      instance = new HiveContext(sparkContext)
    }
    instance
  }
}