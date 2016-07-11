package cn.cnic.bigdatalab

import cn.cnic.bigdatalab.Task.{OfflineTask, RealTimeTask, StoreTask, TaskBean}
import cn.cnic.bigdatalab.collection.{AgentChannel, AgentSink, AgentSource}
import cn.cnic.bigdatalab.datachain._
import cn.cnic.bigdatalab.entity.Schema
import cn.cnic.bigdatalab.transformer.{TMapping}
import cn.cnic.bigdatalab.utils.{FileUtil, PropertyUtil}
import cn.cnic.bigdatalab.transformer.TMapping
import cn.cnic.bigdatalab.utils.PropertyUtil
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.security.authentication.util.KerberosName
import org.apache.hadoop.mapred.JobConf

/**
  * Created by xjzhu@cnic.cn on 2016/6/20.
  */
abstract class AbstractDataChainTestSuit extends FunSuite with BeforeAndAfterAll{

  val json_path = PropertyUtil.getPropertyValue("json_path")

  val mapping_conf = "/opt/mappingConf.json"

  val sql = "insert into table user select name, age from test"
  val sql1 = "insert into table user select name, age from test1"
  val sqlAny = "insert into table user select * from test"
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
  var mysqlStoreTableSchema:Schema = new Schema()
  var mongodbTableSchema = new Schema()
  var hiveTableSchema = new Schema()
  var hiveTest1Schema = new Schema()
  var solrTableSchema = new Schema()

  //streaming table schema params
  val streamingTable: String = "test"
  val streamingColumns = Map("id" ->"int", "name" -> "string", "age" -> "int")

  //mysql table schema params
  val mysqlDB: String = "spark"
  val mysqlTable: String = "user"
  val mysqlColumns = Map("name"->"string", "age"->"int")

  //mysql table schema params
  val mysqlStoreDB: String = "spark"
  val mysqlStoreTable: String = "user1"
  val mysqlStoreColumns = Map("id" -> "int", "name"->"string", "age"->"int")

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

  //solr table schema params
  val solrTable: String = "user"
  val solrColumns = Map("id" ->"int", "name"->"string", "age" -> "int")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    streamingTableSchema.setDriver("streaming").setTable(streamingTable).setColumns(streamingColumns)
    mysqlStoreTableSchema.setDriver("mysql").setDb(mysqlStoreDB).setTable(mysqlStoreTable).setColumns(mysqlStoreColumns)
    mysqlTableSchema.setDriver("mysql").setDb(mysqlDB).setTable(mysqlTable).setColumns(mysqlColumns)
    mongodbTableSchema.setDriver("mongodb").setDb(mongoDatabase).setTable(mongoTable).setColumns(mongoColumns)
    hiveTableSchema.setDriver("hive").setTable(hiveTable).setColumns(hiveColumns)
    hiveTest1Schema.setDriver("hive").setTable(hiveTable1).setColumns(hiveColumns)
    solrTableSchema.setDriver("solr").setTable(solrTable).setColumns(solrColumns)

  }

  override protected def afterAll(): Unit = {
    try {
      //sqlContext.sparkContext.stop()
    } finally {
      super.afterAll()
    }
  }


  /*test("Chain: hive->mysql") {

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
    val task = new TaskBean().initRealtime(name, sql, topic, streamingTableSchema, mysqlTableSchema, "D:\\DataChain\\conf\\csvMapping.json")
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
    val mapping: TMapping = new TMapping()

    //3. Define real Task
    val task = new TaskBean().initStore(name, topic, streamingTableSchema, mysqlStoreTableSchema, "D:\\DataChain\\conf\\csvMapping.json")
    //val taskBean = new TaskBean().initOffline(name, sql1, hiveTest1Schema, mysqlTableSchema)


    val collectionStep = new CollectionStep().initAgent(agentName, agentHost, agentUsername, agentPassword).setChannel(channel).setSource(source).setSink(sink)
    val taskStep = new TaskStep().setStoreTask(new StoreTask(task))


    val chain = new Chain()
    chain.addStep(collectionStep).addStep(taskStep).run()
  }

  test("Chain: csv->kafka->realTime->solr") {

    //1. Define agent source & sink
    val channel = new AgentChannel(agentChannel, channelParameters)
    val source = new AgentSource(agentSource, sourceParameters)
    val sink = new AgentSink(agentSink, kafkaSinkParameters)

    //2. Define Mapping
    //    val mapping:TMapping = new TMapping()

    //3. Define real Task
    val task = new TaskBean().initRealtime(name, sqlAny, topic, streamingTableSchema, solrTableSchema, "D:\\DataChain\\conf\\csvMapping.json")


    val collectionStep = new CollectionStep().initAgent(agentName,agentHost,agentUsername, agentPassword).setChannel(channel).setSource(source).setSink(sink)

    val taskStep = new TaskStep().setRealTimeTask(new RealTimeTask(task))


    val chain = new Chain()
    chain.addStep(collectionStep).addStep(taskStep).run()
  }*/


/*  //use json file
  test("Chain By JSON: csv->kafka->realTime->mysql") {



  /*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~Store~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
  test("Chain By JSON: hive->mysql") {

    //1. Define Task

    val task_json_path = json_path + "/" + "offline/" + "offlineTask_hive2mysql.json"
    val taskBean = FileUtil.taskReader(task_json_path)
    val taskStep = new TaskStep().setOfflineTask(new OfflineTask(taskBean))

    val chain = new Chain()
    chain.addStep(taskStep).run()

  }

  test("Chain By JSON: mysql->mongo") {

    //1. Define Task

    val task_json_path = json_path + "offline/" + "offlineTask_mysql2mongo.json"
    val taskBean = FileUtil.taskReader(task_json_path)
    val taskStep = new TaskStep().setOfflineTask(new OfflineTask(taskBean))

    val chain = new Chain()
    chain.addStep(taskStep).run()

  }

  test("Chain By JSON: mysql->solr") {

    //1. Define Task

    val task_json_path = json_path + "offline/" + "offlineTask_mysql2solr.json"
    val taskBean = FileUtil.taskReader(task_json_path)
    val taskStep = new TaskStep().setOfflineTask(new OfflineTask(taskBean))

    val chain = new Chain()
    chain.addStep(taskStep).run()

  }

  test("Chain By JSON: mysql->hbase") {

    //1. Define Task

    val task_json_path = json_path + "offline/" + "offlineTask_mysql2hbase.json"
    val taskBean = FileUtil.taskReader(task_json_path)
    val taskStep = new TaskStep().setOfflineTask(new OfflineTask(taskBean))

    val chain = new Chain()
    chain.addStep(taskStep).run()

  }


  /*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~Store~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
  test("Chain By Json: csv->kafka->store") {

    //1.define Collection
    val agent_json_path = json_path + "/" + "agent.json"
    val agent = FileUtil.agentReader(agent_json_path)
    val collectionStep = new CollectionStep().initAgent(agent)


    //3. Define store Task
    val task_json_path = json_path + "/" + "storeTask.json"
    val taskBean = FileUtil.taskReader(task_json_path)
    val taskStep = new TaskStep().setStoreTask(new StoreTask(taskBean))

    val chain = new Chain()
    chain.addStep(collectionStep).addStep(taskStep).run()

  }


  /*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~RealTime~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
  //use json file
  test("Chain By JSON: csv->kafka->realTime->mysql") {

    //1.define Collection
    val agent_json_path = json_path + "/" + "agent.json"
    val agent = FileUtil.agentReader(agent_json_path)
    val collectionStep = new CollectionStep().initAgent(agent)

    //2. Define real Task
    val task_json_path = json_path + "/" + "realtime/realTimeTask.json"
    val taskBean = FileUtil.taskReader(task_json_path)
    val taskStep = new TaskStep().setRealTimeTask(new RealTimeTask(taskBean))


    val chain = new Chain()
    chain.addStep(collectionStep).addStep(taskStep).run()
  }

  //use json file
  test("Chain By JSON: csv->kafka->realTime->mysql") {
    //1.define Collection
    val agent_json_path = json_path + "/" + "agent.json"
    val agent = FileUtil.agentReader(agent_json_path)
    val collectionStep = new CollectionStep().initAgent(agent)

    //2. Define real Task
    val task_json_path = json_path + "/realtime/" + "realTimeMultiTableTask.json"
    val taskBean = FileUtil.taskReader(task_json_path)
    val taskStep = new TaskStep().setRealTimeTask(new RealTimeTask(taskBean))


    val chain = new Chain()
    chain.addStep(collectionStep).addStep(taskStep).run()
  }

  //use json file
  test("Chain By JSON: csv->kafka->realTime->hive") {
    //1.define Collection
    val agent_json_path = json_path + "/" + "agent.json"
    val agent = FileUtil.agentReader(agent_json_path)
    val collectionStep = new CollectionStep().initAgent(agent)

    //2. Define real Task
    val task_json_path = json_path + "/realtime/" + "realTimeTask_hive.json"
    val taskBean = FileUtil.taskReader(task_json_path)
    val taskStep = new TaskStep().setRealTimeTask(new RealTimeTask(taskBean))


    val chain = new Chain()
    chain.addStep(collectionStep).addStep(taskStep).run()
  }

  //use json file
  test("Chain By JSON: csv->kafka->realTime->solr") {
    //1.define Collection
    val agent_json_path = json_path + "/" + "agent.json"
    val agent = FileUtil.agentReader(agent_json_path)
    val collectionStep = new CollectionStep().initAgent(agent)

    //2. Define real Task
    val task_json_path = json_path + "/realtime/" + "realTimeTask_solr.json"
    val taskBean = FileUtil.taskReader(task_json_path)
    val taskStep = new TaskStep().setRealTimeTask(new RealTimeTask(taskBean))


    val chain = new Chain()
    chain.addStep(collectionStep).addStep(taskStep).run()
  }

//  use json file
  test("Chain By JSON: csv->kafka->realTime->mongodb") {
    //1.define Collection
    val agent_json_path = json_path + "/" + "agent.json"
    val agent = FileUtil.agentReader(agent_json_path)
    val collectionStep = new CollectionStep().initAgent(agent)

    //2. Define real Task
    val task_json_path = json_path + "/realtime/" + "realTimeTask_mongodb.json"
    val taskBean = FileUtil.taskReader(task_json_path)
    val taskStep = new TaskStep().setRealTimeTask(new RealTimeTask(taskBean))


    val chain = new Chain()
    chain.addStep(collectionStep).addStep(taskStep).run()
  }

  //use json file
  test("Chain By JSON: csv->kafka->realTime->hbase") {
    //1.define Collection
    val agent_json_path = json_path + "/" + "agent.json"
    val agent = FileUtil.agentReader(agent_json_path)
    val collectionStep = new CollectionStep().initAgent(agent)

    //2. Define real Task
    val task_json_path = json_path + "/realtime/" + "realTimeTask_hbase.json"
    val taskBean = FileUtil.taskReader(task_json_path)
    val taskStep = new TaskStep().setRealTimeTask(new RealTimeTask(taskBean))


    val chain = new Chain()
    chain.addStep(collectionStep).addStep(taskStep).run()
  }*/

  //use json file
  test("Chain By JSON: csv->kafka->realTime->impala") {
    //1.define Collection
    val agent_json_path = json_path + "/" + "agent.json"
    val agent = FileUtil.agentReader(agent_json_path)
    val collectionStep = new CollectionStep().initAgent(agent)

    //2. Define real Task
    val task_json_path = json_path + "/realtime/" + "realTimeTask_impala.json"
    val taskBean = FileUtil.taskReader(task_json_path)
    val taskStep = new TaskStep().setRealTimeTask(new RealTimeTask(taskBean))


    val chain = new Chain()
    chain.addStep(collectionStep).addStep(taskStep).run()
  }

}

class DataChainTestSuit extends AbstractDataChainTestSuit{

}
