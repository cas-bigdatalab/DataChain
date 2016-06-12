package com.cnic.datachain.dataparser

import org.apache.spark.SparkConf

/**
 * Created by cnic on 2016/5/24.
 */
object Test {
  def main(args: Array[String]) {



    ///注意setMaster("local")这行代码，表明Spark以local运行(注意local与standalone模式的区别)

    //System.setProperty("hadoop.home.dir", "D:\\hadoop-2.6.0")

    // val conf = new SparkConf().setAppName("SparkWordCount").setMaster("local")

    //val conf = new SparkConf().setAppName("Hdfs").setMaster("local[2]").set("spark.executor.memory","1g")


    val conf = new SparkConf().setAppName("testWordCount")

      .setMaster("spark://10.0.71.32:7077")//spark集群的url

      .setSparkHome("/opt/spark-1.6.1-bin-hadoop2.6")//spark集群的home路径
      .set("spark.executor.memory","2g")//executor 可以利用的内存大小
      .set("spark.driver.memory","2g")
      .setJars(Array("E:\\DataChain\\out\\artifacts\\datachain_jar\\datachain.jar"))//开发环境中自己开发的spark作业代码生成的jar文件所在目录

    //val sc = new org.apache.spark.SparkContext("", "WordCount",System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_TEST_JAR")))

    val sc = new org.apache.spark.SparkContext(conf)

    // sc.addFile("hdfs://192.168.20.201:9000/flume/logistics/135954_450_kD4l_riIsdhcq")

    val log = sc.textFile("hdfs://10.0.71.32:9000/tmp/input/test.txt")//从hdfs上的/input目录中读取文           件进行计数

    // val sc = new SparkContext(conf)

    // val rdd = sc.textFile("file:///D:/TDDOWNLOAD/012312_927_X71q_xWFhjY1u")

    log.flatMap(_.split(" "))
      .map((_, 1)).reduceByKey(_+_)
      .map(x => (x._2, x._1))
      .sortByKey(false)
      .map(x => (x._2, x._1))
      .saveAsTextFile("hdfs://10.0.71.32:9000/tmp/output")//计数结果在hdfs上的保存目录    sc.stop

  }
}
