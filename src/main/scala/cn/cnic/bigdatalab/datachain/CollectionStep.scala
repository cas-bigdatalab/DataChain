package cn.cnic.bigdatalab.datachain

import cn.cnic.bigdatalab.collection._
import org.apache.flume.{Sink, Source}

/**
  * Created by cnic on 2016/6/21.
  */
class CollectionStep extends Step{

  private var agent: Agent = _

  def initAgent(name: String, host:String): CollectionStep ={
    if (agent == null) {
      agent = new Agent(name,host)
    }
    this
  }

  def setSource(src:AgentSource): CollectionStep ={
    if (agent != null) agent.setAgentSource(src)
    this
  }

  def setSink(sk:AgentSink): CollectionStep ={
    if (agent != null) agent.setAgentSink(sk)
    this
  }

  def setChannel(ch: AgentChannel): CollectionStep ={
    if (agent != null) agent.setAgentChannel(ch)
    this
  }

  override def run(): Unit ={
    new AgentScheduler(agent).launch()
  }

}
