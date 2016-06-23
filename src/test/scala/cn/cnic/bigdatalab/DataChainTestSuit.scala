package cn.cnic.bigdatalab

import cn.cnic.bigdatalab.Collection.{AgentSink, AgentSource, HdfsSink, SpoolDirSource}
import cn.cnic.bigdatalab.Task.{OfflineTask, RealTimeTask, StoreTask}
import cn.cnic.bigdatalab.datachain._
import cn.cnic.bigdatalab.entity.Schema
import cn.cnic.bigdatalab.transformer.Mapping
import com.github.casbigdatalab.datachain.transformer.csvtransformer
import org.apache.flume.sink.AvroSink
import org.apache.flume.source.{DefaultSourceFactory, SpoolDirectorySource}
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

  val sinkConf = Map(
    "type" -> "org.apache.flume.sink.kafka.KafkaSink",
    "brokerList" -> "NameNode:9092,DataNode-1:9092,DataNode-2:9092",
    "topic" -> "Test"
  )

  val sourceConf = Map(
    "type" -> "spooldir", "spooldir" -> "/opt/flumeTest"
  )

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

    val source = new AgentSource("src1", sourceConf)
    val sink = new AgentSink("sink1", sinkConf)
    val schema = new Schema()

    val mapping:Mapping = new Mapping()
    val collectionStep = new CollectionStep().initAgent("test").setSource(source).setSink(sink)
    val transformerStep = new TransformerStep().setTransformer(mapping)
    //val taskStep = new TaskStep().setOfflineTask(new OfflineTask(sql))
    val taskStep = new TaskStep().setRealTimeTask(new RealTimeTask(sql, topic, schema, transformerStep.getTransformer()))

    val chain = new Chain()
    chain.addStep(collectionStep).addStep(transformerStep).addStep(taskStep).run()
  }
}
