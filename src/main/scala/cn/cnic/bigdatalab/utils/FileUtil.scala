package cn.cnic.bigdatalab.utils

import java.io.File

import cn.cnic.bigdatalab.collection.{Agent, AgentChannel, AgentSink, AgentSource}
import cn.cnic.bigdatalab.compute.realtime.{InterpreterSingleton, Utils}

import cn.cnic.bigdatalab.task.TaskBean
import cn.cnic.bigdatalab.collection.{AgentSink, AgentChannel, AgentSource, Agent}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.parsing.json.JSON

/**
  * Created by xjzhu on 16/7/4.
  */
object FileUtil {


  def fileReader(filePath: String):String={

    var str = ""
    val file = Source.fromFile(filePath)
    val iter = file.buffered
    while (iter.hasNext) {
      val line = iter.head
      str += line
      iter.next()
    }
    file.close()
    str
  }

  def dirRead(dirPath: String): ArrayBuffer[String] = {
    val dirFile = new File(dirPath)
    val content: ArrayBuffer[String] = new ArrayBuffer[String]()
    dirFile.listFiles().filter(_.isFile).foreach(file =>{
        content.append(fileReader(file.getAbsolutePath))
    }
    )
    content
  }

  def fileWriting(path: String, str: String) {
    import java.io.{File, PrintWriter}
    val pw = new PrintWriter(new File(path))
    pw.append(s"str: $str").write("\n")
    pw.flush
    pw.close
  }

  def agentReader(agentPath:String): Agent ={
    //val agentPath = Thread.currentThread().getContextClassLoader.getResource("agent.json").getPath
    val agent = Agent.parseJson(fileReader(agentPath))
    agent
  }

  def taskReader(taskPath:String): TaskBean = {
    //val taskPath = Thread.currentThread().getContextClassLoader.getResource("realTimeTask.json").getPath
    val taskBean = TaskBean.parseJson(fileReader(taskPath))
    taskBean
  }



  def main(args: Array[String]): Unit ={

    //schema
    /*val path = Thread.currentThread().getContextClassLoader.getResource("userSchema.json").getPath
    val schema = schemaReader(path)*/

    //agent
    /*val agentPath = Thread.currentThread().getContextClassLoader.getResource("agent.json").getPath
    val agent = Agent.parseJson(fileReader(agentPath))*/

    //task
//    val taskPath = Thread.currentThread().getContextClassLoader.getResource("realtime/realTimeTask.json").getPath
//    val realtimeTaskBean = TaskBean.parseJson(fileReader(taskPath))
//
//    realtimeTaskBean.getName()

    //read dir
    val files = FileUtil.dirRead("D:\\git\\DataChain\\externalcode")
    val schemal = "id:int,name:string,age:int"
    val line = "1,dyy,29"
    val mainClass = "ConnectMySql"

    val interpreter = InterpreterSingleton.getInstance()
    files.reverse.foreach(interpreter.compileString(_))
    interpreter.interpret(s"""new $mainClass().process("$schemal", "$line")""")

//    fileWriting("D:/dyy.txt", "aaa")
//    fileWriting("D:/dyy.txt", "bbb")

  }




}
