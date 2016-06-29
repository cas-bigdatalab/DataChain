package cn.cnic.bigdatalab

import cn.cnic.bigdatalab.Task.{OfflineTask, RealTimeTask, StoreTask, TaskBean}
import cn.cnic.bigdatalab.collection.{AgentChannel, AgentSink, AgentSource}
import cn.cnic.bigdatalab.datachain._
import cn.cnic.bigdatalab.entity.Schema
import cn.cnic.bigdatalab.transformer.Mapping
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * Created by xjzhu@cnic.cn on 2016/6/20.
  */
abstract class AbstractDataChainTestSuit extends FunSuite with BeforeAndAfterAll{

  val mapping_conf = "/opt/mappingConf.json"

  val sql = "insert into user select * from test"
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

  var mysqlTableSchema:Schema = new Schema()
  var mongodbTableSchema = new Schema()
  var hiveTableSchema = new Schema()

  //mysql table schema params
  val mysqlDB: String = "spark"
  val mysqlTable: String = "user"
  val mysqlColumns = Map("name"->"string", "age"->"int")

  //mongodb table schema params
  val mongoDatabase: String = "spark"
  val mongoTable: String = "student"
  val mongoColumns = Map("name"->"string", "age"->"int")

  //hive table schema params
  val hiveTable: String = "test"
  val hiveColumns = Map("name"->"STRING", "age"->"INT")


  override protected def beforeAll(): Unit = {
    super.beforeAll()
    mysqlTableSchema.setDriver("mysql").setDb(mysqlDB).setTable(mysqlTable).setColumns(mysqlColumns)
    mongodbTableSchema.setDriver("mongodb").setDb(mongoDatabase).setTable(mongoTable).setColumns(mongoColumns)
    hiveTableSchema.setDriver("hive").setTable(hiveTable).setColumns(hiveColumns)

  }

  override protected def afterAll(): Unit = {
    try {
      //sqlContext.sparkContext.stop()
    } finally {
      super.afterAll()
    }
  }



  test("Chain: csv->kafka->realTime->mongodb") {

    //1. Define agent source & sink
    val channel = new AgentChannel(agentChannel, channelParameters)
    val source = new AgentSource(agentSource, sourceParameters)
    val sink = new AgentSink(agentSink, sinkParameters)

    //2. Define Mapping
    val mapping:Mapping = new Mapping()

    //3. Define Task
    //val task = new TaskInstance().init(name, taskType, sql, topic, mysqlTableSchema, mapping.toString)
    val taskBean = new TaskBean().initOffline(name, sql, hiveTableSchema, mysqlTableSchema)

    //val collectionStep = new CollectionStep().initAgent(agentName,agentHost).setChannel(channel).setSource(source).setSink(sink)
    //val transformerStep = new TransformerStep().setTransformer(mapping)
    val taskStep = new TaskStep().setOfflineTask(new OfflineTask(taskBean)).run
    //val taskStep = new TaskStep().setRealTimeTask(new RealTimeTask(task))

    Thread.sleep(20 * 1000)

    //val chain = new Chain()
    //chain.addStep(collectionStep).addStep(transformerStep).addStep(taskStep).run()
  }
}

class DataChainTestSuit extends AbstractDataChainTestSuit{

}
