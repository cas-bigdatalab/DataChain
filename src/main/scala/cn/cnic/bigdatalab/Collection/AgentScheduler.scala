package cn.cnic.bigdatalab.collection

import java.io.{PrintWriter, File}
import cn.cnic.bigdatalab.utils.{SshUtil, PropertyUtil}




/**
  * Created by cnic on 2016/6/21.
  */
class AgentScheduler(agent: Agent) {

  def launch(): Unit ={

    //generate conf file
    createConfFile(getContent().toString)

    //copy confFile to flume agent conf path
    copyConf2Server()

    //launch flume-ng
    runFlumeOnServer()

  }

  private def getContent() : StringBuffer ={
    val content: StringBuffer = new StringBuffer()

    //define source, sink, channel name
    val defineSource = agent.getName() + ".sources = " + agent.getAgentSource().getName() + "\n"
    content.append(defineSource)
    val defineChannel = agent.getName() + ".channels = " + agent.getAgentChannel().getName() + "\n"
    content.append(defineChannel)
    val defineSink = agent.getName() + ".sinks = " + agent.getAgentSink().getName() + "\n"
    content.append(defineSink)

    content.append("\n")

    //configure channel
    for((key, value) <- agent.getAgentChannel().getParameters()){
      content.append(agent.getName() + ".channels." + agent.getAgentChannel().getName() + "." + key + " = " + value + "\n")
    }

    content.append("\n")

    //configure source
    for((key, value) <- agent.getAgentSource().getParameters()){
      content.append(agent.getName() + ".sources."  + agent.getAgentSource().getName() + "." + key + " = " + value + "\n")
    }

    content.append("\n")

    //configure sink
    for((key, value) <- agent.getAgentSink().getParameters()){
      content.append(agent.getName() + ".sinks." + agent.getAgentSink().getName() + "." + key + " = " + value + "\n")
    }
    content
  }

  private def createConfFile(content : String): Unit ={
    //Generate configure file
    val confFilePath = getConfFilePath()
    val confWriter = new PrintWriter(new File(confFilePath))
    confWriter.write(content)
    confWriter.close()

  }

  private def copyConf2Server(): Unit ={
    //val copyConfFileCmd = "scp " + getConfFilePath() + "root@" + agent.getHost() + ":" + PropertyUtil.getPropertyValue("flume_home") + "/conf/"
    val srcFile = getConfFilePath()
    val destFile = PropertyUtil.getPropertyValue("flume_home") +"/conf"
    SshUtil.scp(srcFile,destFile, agent.getHost(),agent.getUserName(),agent.getPassword())


  }

  private def runFlumeOnServer(): Unit ={
    //val launchCmd =  "ssh root@" + agent.getHost() + " /bin/bash cd " + flumeHome + ";" + "bin/flume-ng agent --conf conf --conf-file conf/" + getConfFileName() + " --name " + agent.getName() + "-Dflume.root.logger=INFO,console"
    /*val cdCmd = "cd " + PropertyUtil.getPropertyValue("flume_home")
    val flumeCmd = "bin/flume-ng agent --conf conf --conf-file conf/" + getConfFileName() + " --name " + agent.getName() + " -Dflume.root.logger=INFO,LOGFILE"

    val command = cdCmd + " && " + flumeCmd
    Process(Seq("bash","-c",command)).!*/

    val command: StringBuffer = new StringBuffer()
    /*command.append("cd ").append(PropertyUtil.getPropertyValue("flume_home")).append(";")
      .append("./bin/flume-ng agent --conf conf ")
      .append("--conf-file conf/").append(getConfFileName()).append(" ")
      .append("--name ").append(agent.getName()).append(" ")
      .append("-Dflume.root.logger=INFO,console")*/

    //command.append("cd ").append(PropertyUtil.getPropertyValue("flume_home")).append(";")
      .append("/bin/bash " + PropertyUtil.getPropertyValue("flume_home") + "/bin/flume-ng agent --conf conf ")
      .append("--conf-file "+ PropertyUtil.getPropertyValue("flume_home")+ "/conf/").append(getConfFileName()).append(" ")
      .append("--name ").append(agent.getName()).append(" ")
      .append("> "+ PropertyUtil.getPropertyValue("flume_log_dir") + " 2>&1 &")

    println(command.toString)

    SshUtil.exec(command.toString, agent.getHost(), agent.getUserName(), agent.getPassword())


  }

  private def getConfFilePath():String ={

    val agentConfPath = PropertyUtil.getPropertyValue("flume_conf_localDir") + "/" + getConfFileName()
    agentConfPath
  }

  private def getConfFileName():String = {
    agent.getName() + ".properties"
  }

}
