package cn.cnic.bigdatalab

import cn.cnic.bigdatalab.Task.{OfflineTask, RealTimeTask, StoreTask, TaskBean}
import cn.cnic.bigdatalab.collection.{AgentChannel, AgentSink, AgentSource}
import cn.cnic.bigdatalab.datachain._
import cn.cnic.bigdatalab.entity.Schema
import cn.cnic.bigdatalab.transformer.{TMapping, Mapping}
import cn.cnic.bigdatalab.utils.{FileUtil, PropertyUtil}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * Created by xjzhu@cnic.cn on 2016/6/20.
  */
abstract class AbstractDataChainTestSuit extends FunSuite with BeforeAndAfterAll{

  val json_path = PropertyUtil.getPropertyValue("json_path")
  val agent_json_path = json_path + "/" + "agent.json"
  val task_json_path = json_path + "/" + "task.json"
  val mapping_conf = "/opt/mappingConf.json"

  val sql = "insert into table user select name, age from test"
  val sql1 = "insert into table user select name, age from test1"
  val topic = "test"
  val name = "test"
  val taskType = "realtime"

  //agent related parameters
  val agentHost = "10.0.50.216"
  val agentUsername = "root"
  val agentPassword = "bigdata"
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

  val kafkaSinkParameters = Map(
    "channel" -> "channel1",
    "type" -> "org.apache.flume.sink.kafka.KafkaSink",
    "brokerList" -> PropertyUtil.getPropertyValue("kafka.brokerList"),
    "topic" -> "test"
  )
  val sourceParameters = Map(
    "channels" -> "channel1",
    "type" -> "spooldir",
    "spoolDir" -> "/opt/flumeSooldir",
    "fileHeader" -> "true"
  )

  var streamingTableSchema = new Schema()
  var mysqlTableSchema:Schema = new Schema()
  var mongodbTableSchema = new Schema()
  var hiveTableSchema = new Schema()
  var hiveTest1Schema = new Schema()

  //streaming table schema params
  val streamingTable: String = "test"
  val streamingColumns = Map("id" ->"int", "name" -> "string", "age" -> "int")

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
  val hiveTable1: String = "test1"

  //transformer mapping
  val mappingJson = ""


  override protected def beforeAll(): Unit = {
    super.beforeAll()
    streamingTableSchema.setDriver("streaming").setTable(streamingTable).setColumns(streamingColumns)
    mysqlTableSchema.setDriver("mysql").setDb(mysqlDB).setTable(mysqlTable).setColumns(mysqlColumns)
    mongodbTableSchema.setDriver("mongodb").setDb(mongoDatabase).setTable(mongoTable).setColumns(mongoColumns)
    hiveTableSchema.setDriver("hive").setTable(hiveTable).setColumns(hiveColumns)
    hiveTest1Schema.setDriver("hive").setTable(hiveTable1).setColumns(hiveColumns)

  }

  override protected def afterAll(): Unit = {
    try {
      //sqlContext.sparkContext.stop()
    } finally {
      super.afterAll()
    }
  }



  test("Chain: hive->mysql") {


    //1. Define Task
    val taskBean = new TaskBean().initOffline(name, sql1, hiveTest1Schema, mysqlTableSchema)
    val taskStep = new TaskStep().setOfflineTask(new OfflineTask(taskBean))

    val chain = new Chain()
    chain.addStep(taskStep).run()
  }

  test("Chain: csv->kafka->realTime->mysql") {

    //1. Define agent source & sink
    val channel = new AgentChannel(agentChannel, channelParameters)
    val source = new AgentSource(agentSource, sourceParameters)
    val sink = new AgentSink(agentSink, kafkaSinkParameters)

    //2. Define Mapping
    val mapping:TMapping = new TMapping(mappingJson)

    //3. Define real Task
    val task = new TaskBean().initRealtime(name, sql, topic, streamingTableSchema, mysqlTableSchema, "mapping")
    //val taskBean = new TaskBean().initOffline(name, sql1, hiveTest1Schema, mysqlTableSchema)


    val collectionStep = new CollectionStep().initAgent(agentName,agentHost,agentUsername, agentPassword).setChannel(channel).setSource(source).setSink(sink)
    val taskStep = new TaskStep().setRealTimeTask(new RealTimeTask(task))


    val chain = new Chain()
    chain.addStep(collectionStep).addStep(taskStep).run()
  }

  test("Chain: csv->kafka->store") {

    //1. Define agent source & sink
    val channel = new AgentChannel(agentChannel, channelParameters)
    val source = new AgentSource(agentSource, sourceParameters)
    val sink = new AgentSink(agentSink, kafkaSinkParameters)

    //2. Define Mapping
    val mapping:Mapping = new Mapping()

    //3. Define real Task
    val task = new TaskBean().initStore(name, topic, streamingTableSchema, mysqlTableSchema, "mapping")
    //val taskBean = new TaskBean().initOffline(name, sql1, hiveTest1Schema, mysqlTableSchema)


    val collectionStep = new CollectionStep().initAgent(agentName,agentHost,agentUsername, agentPassword).setChannel(channel).setSource(source).setSink(sink)
    val taskStep = new TaskStep().setStoreTask(new StoreTask(task))


    val chain = new Chain()
    chain.addStep(collectionStep).addStep(taskStep).run()
  }


  //use json file
  test("Chain: csv->kafka->realTime->mysql") {

    //1.define Collection
    val agent = FileUtil.agentReader(agent_json_path)
    val collectionStep = new CollectionStep().initAgent(agent)

    //2. Define real Task
    val taskBean = FileUtil.taskReader(task_json_path)
    val taskStep = new TaskStep().setRealTimeTask(new RealTimeTask(taskBean))


    val chain = new Chain()
    chain.addStep(collectionStep).addStep(taskStep).run()
  }

}

class DataChainTestSuit extends AbstractDataChainTestSuit{

}
