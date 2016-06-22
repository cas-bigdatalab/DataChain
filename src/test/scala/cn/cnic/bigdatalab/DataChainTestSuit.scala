package cn.cnic.bigdatalab

import cn.cnic.bigdatalab.Collection.{HdfsSink, SpoolDirSource}
import cn.cnic.bigdatalab.Task.{StoreTask, OfflineTask, RealTimeTask}
import cn.cnic.bigdatalab.datachain._

import com.github.casbigdatalab.datachain.transformer.csvtransformer
import org.apache.flume.sink.AvroSink
import org.apache.flume.source.{SpoolDirectorySource, DefaultSourceFactory}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * Created by xjzhu@cnic.cn on 2016/6/20.
  */
abstract class DataChainTestSuit extends FunSuite with BeforeAndAfterAll{

  val sourceJsonStr = """'source':{
                            |'name':'src1',
                            |'type':'spooldir',
                            |'spooldir':'/opt/flumeTest'}""".stripMargin

  val sinkjsonStr = """'sink':{
                          |'name':'sink1',
                          |'type':'org.apache.flume.sink.kafka.KafkaSink',
                          |'brokerList':'NameNode:9092,DataNode-1:9092,DataNode-2:9092',
                          |'topic:'Test'}}"""".stripMargin

  val mapping_conf = "/opt/mappingConf.json"

  val sql = "insert into mysqlTable select * from src"
  val topic = "Test"


  override protected def beforeAll(): Unit = {
    super.beforeAll()
    //sqlContext = new SQLContext(new SparkContext("local[2]", "MemcachedSuite"))
  }

  override protected def afterAll(): Unit = {
    try {
      //sqlContext.sparkContext.stop()
    } finally {
      super.afterAll()
    }
  }

  test("Chain: csv->kafka->realTime->mongodb") {

    val source = new SpoolDirSource("spooldir","src1")
    val sink = new HdfsSink("hdfs","sink1")

    val collectionStep = new CollectionStep().setSource(source).setSink(sink)
    val transformerStep = new TransformerStep().setMappingFile(mapping_conf)
    //val taskStep = new TaskStep().setOfflineTask(new OfflineTask(sql))
    val taskStep = new TaskStep().setRealTimeTask(new RealTimeTask(sql,topic))

    val chain = new Chain()
    chain.addStep(collectionStep).addStep(transformerStep).addStep(taskStep).run()
  }
}
