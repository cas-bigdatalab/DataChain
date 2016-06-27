package cn.cnic.bigdatalab

import cn.cnic.bigdatalab.Task.{OfflineTask, RealTimeTask, StoreTask, TaskInstance}
import cn.cnic.bigdatalab.collection.{AgentChannel, AgentSink, AgentSource}
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
abstract class AbstractDataChainTestSuit extends FunSuite with BeforeAndAfterAll{

  val mapping_conf = "/opt/mappingConf.json"

  val sql = "insert into mysqlTable select * from src"
  val topic = "Test"
  val name = "test"
  val taskType = "realtime"

  //agent related parameters
  val agentHost = "172.16.106.3"
  val agentName = "spoolAgent"
  val agentChannel = "channel1"
  val agentSource = "src1"
  val agentSink = "sink1"
  val channelParameters = Map(
    "type" -> "memory"
  )
  val sinkParameters = Map(
    "channel" -> "channel1",
    "type" -> "logger"
  )
  val sourceParameters = Map(
    "channels" -> "channel1",
    "type" -> "spooldir",
    "spoolDir" -> "/tmp/flumeSourceDir",
    "fileHeader" -> "true"
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

    //1. Define table schema
    val schema = new Schema()

    //2. Define agent source & sink
    val channel = new AgentChannel(agentChannel, channelParameters)
    val source = new AgentSource(agentSource, sourceParameters)
    val sink = new AgentSink(agentSink, sinkParameters)

    val mapping:Mapping = new Mapping()

    val task = new TaskInstance().init(name, taskType, sql, topic, schema, mapping.toString)



    val collectionStep = new CollectionStep().initAgent(agentName,agentHost).setChannel(channel).setSource(source).setSink(sink)
    val transformerStep = new TransformerStep().setTransformer(mapping)
    //val taskStep = new TaskStep().setOfflineTask(new OfflineTask(sql))
    val taskStep = new TaskStep().setRealTimeTask(new RealTimeTask(task))

    val chain = new Chain()
    chain.addStep(collectionStep).addStep(transformerStep).addStep(taskStep).run()
  }
}

class DataChainTestSuit extends AbstractDataChainTestSuit{

}
