package cn.cnic.bigdatalab.utils

import java.io.File

import cn.cnic.bigdatalab.task.factory.{Json2Task, TaskBean, TaskFactory}
import cn.cnic.bigdatalab.collection.Agent
import cn.cnic.bigdatalab.compute.realtime.utils.Utils

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

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
    val taskBean = Json2Task.parseJson(fileReader(taskPath))
    taskBean
  }



  def main(args: Array[String]): Unit ={

    //read dir
//    val files = FileUtil.dirRead("D:\\git\\DataChain\\externalcode")
    val schemal = "id:int,name:string,age:int"
    val line = "1,dyy,29"
    val mainClass = "ConnectMySql"
    val dclass = "cnic.bigdata.external.ConnectMySql"
//
//    val interpreter = InterpreterSingleton.getInstance()
//    interpreter.addImports("cnic.bigdata.external.ConnectMySql")
////    interpreter.interpret("import cnic.bigdata.external.ConnectMySql")
////    files.reverse.foreach(interpreter.compileString(_))
//    interpreter.interpret(s"""new $mainClass().process("$schemal", "$line")""")

//    fileWriting("D:/dyy.txt", "aaa")
//    fileWriting("D:/dyy.txt", "bbb")

//    import scala.reflect.runtime.universe
//
//    val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
//
//    val module = runtimeMirror.staticModule("cnic.bigdata.external.ConnectMySql")
//
//    val obj = runtimeMirror.reflectModule(module)
//
//    println(obj.instance)

//    val foo  = Class.forName("cnic.bigdata.external.ConnectMySql").newInstance.asInstanceOf[{ def process(schema: String, line: String): Unit }]
//
//    foo.process(schemal, line)

//    val result = Utils.invoker("cn.cnic.bigdatalab.utils.FileUtil$", "fileReader", "D:\\git\\DataChain\\conf\\csvMapping.json")
//    println(result)
//      val result = Utils.invoker("cnic.bigdata.external.ConnectMySql", "process", schemal, line)
    Utils.invokeStaticMethod("cnic.bigdata.external.TestMysql", PropertyUtil.getPropertyValue("sdk_method"), schemal, line)

  }




}
