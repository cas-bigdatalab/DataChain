package cn.cnic.bigdatalab.Collection

/**
  * Created by cnic on 2016/6/21.
  */
class AgentScheduler(agent: Agent) {

  def launch(): Unit ={
    val content = getContent()
    createConfFile(content.toString)

    //远程启动flume agent

  }

  private def getContent() : StringBuffer ={
    val content: StringBuffer = new StringBuffer()

    //define source, sink, channel name
    val defineSource = agent.getName() + ".sources = " + agent.getAgentSource().getName() + "\n"
    content.append(defineSource)
    val defineSink = agent.getName() + ".sinks = " + agent.getAgentSink().getName() + "\n"
    content.append(defineSink)

    //configure source
    for((key, value) <- agent.getAgentSource().getConf()){
      content.append(agent.getName() + ".sources." + key + " = " + value + "\n")
    }

    //configure sink
    for((key, value) <- agent.getAgentSink().getConf()){
      content.append(agent.getName() + ".sinks." + key + " = " + value + "\n")
    }
    content
  }

  private def createConfFile(content : String): Unit ={
    //Generate configure file

  }

}
