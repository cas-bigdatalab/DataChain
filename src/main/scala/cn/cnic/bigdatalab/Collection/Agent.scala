package cn.cnic.bigdatalab.collection

import cn.cnic.bigdatalab.collection.{AgentSource,AgentChannel,AgentSink}

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
