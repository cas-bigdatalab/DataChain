package cn.cnic.bigdatalab.collection

import cn.cnic.bigdatalab.collection.{AgentSource,AgentChannel,AgentSink}

import scala.util.parsing.json.JSON

/**
  * Created by Flora on 2016/6/22.
  */
class Agent(name:String, host: String, username: String, password: String) {

  private var source:AgentSource = _
  private var sink:AgentSink = _
  private var channel:AgentChannel = _

  def setAgentSource(source: AgentSource): Unit ={
    this.source = source
  }

  def getAgentSource(): AgentSource ={
    this.source
  }

  def setAgentSink(sink: AgentSink): Unit ={
    this.sink = sink
  }

  def getAgentSink(): AgentSink ={
    this.sink
  }

  def setAgentChannel(channel:AgentChannel): Unit ={
    this.channel = channel
  }

  def getAgentChannel():AgentChannel={
    this.channel
  }

  def getName(): String ={
    name
  }

  def getHost(): String ={
    host
  }

  def getUserName(): String ={
    username
  }

  def getPassword(): String ={
    password
  }
}

object Agent{
  def parseJson(jsonStr: String): Agent = {

    var agent: Agent = null
    var agentHost = ""
    var agentUsername = ""
    var agentPassword = ""
    var agentName = ""

    //val jsonStr = fileReader(filePath);
    val mapping = JSON.parseFull(jsonStr).get
    val map: Map[String, Any] = mapping.asInstanceOf[Map[String, Any]].get("Agent").get.asInstanceOf[Map[String, Any]]

    //agentHost
    assert(!map.get("agentHost").get.asInstanceOf[String].isEmpty)
    agentHost = map.get("agentHost").get.asInstanceOf[String]

    //agentUsername
    assert(!map.get("agentUsername").get.asInstanceOf[String].isEmpty)
    agentUsername = map.get("agentUsername").get.asInstanceOf[String]

    //agentPassword
    assert(!map.get("agentPassword").get.asInstanceOf[String].isEmpty)
    agentPassword = map.get("agentPassword").get.asInstanceOf[String]

    //agentName
    assert(!map.get("agentName").get.asInstanceOf[String].isEmpty)
    agentName = map.get("agentName").get.asInstanceOf[String]

    agent = new Agent(agentName,agentHost,agentUsername,agentPassword)


    assert(!map.get("agentChannel").isEmpty)
    val channelMap : Map[String, Any]= map.get("agentChannel").get.asInstanceOf[Map[String,Any]]
    val channelName = channelMap.get("name").get.asInstanceOf[String]
    val channelParameters = channelMap.get("parameters").get.asInstanceOf[Map[String,String]]
    var channel:AgentChannel = new AgentChannel(channelName, channelParameters)
    agent.setAgentChannel(channel)


    assert(!map.get("agentSource").isEmpty)
    val sourceMap : Map[String, Any]= map.get("agentSource").get.asInstanceOf[Map[String,Any]]
    val sourceName = sourceMap.get("name").get.asInstanceOf[String]
    val sourceParameters = sourceMap.get("parameters").get.asInstanceOf[Map[String,String]]
    var source:AgentSource = new AgentSource(sourceName, sourceParameters)
    agent.setAgentSource(source)

    assert(!map.get("agentSink").isEmpty)
    val sinkMap : Map[String, Any]= map.get("agentSink").get.asInstanceOf[Map[String,Any]]
    val sinkName = sinkMap.get("name").get.asInstanceOf[String]
    val sinkParameters = sinkMap.get("parameters").get.asInstanceOf[Map[String,String]]
    var sink:AgentSink = new AgentSink(sinkName, sinkParameters)
    agent.setAgentSink(sink)

    agent

  }

}
