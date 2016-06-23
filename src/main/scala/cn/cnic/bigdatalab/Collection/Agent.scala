package cn.cnic.bigdatalab.Collection

import com.sun.jndi.cosnaming.IiopUrl.Address

/**
  * Created by Flora on 2016/6/22.
  */
class Agent(name:String, host: String) {

  private var source:AgentSource = _
  private var sink:AgentSink = _


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

  def getName(): String ={
    name
  }

  def getHost(): String ={
    host
  }
}
