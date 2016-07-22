package cn.cnic.bigdatalab.compute.realtime.utils

import scala.tools.nsc.{Interpreter, Settings}

/**
  * Created by Flora on 2016/7/22.
  */
class MyInterpreter(settings : scala.tools.nsc.Settings) extends Interpreter(settings : scala.tools.nsc.Settings) with Serializable

object InterpreterSingleton {

  @transient private var instance: MyInterpreter = _

  def getInstance(): MyInterpreter = {
    if (instance == null) {
      val settings = new Settings(str => println(str))
      settings.usejavacp.value = true
      instance = new MyInterpreter(settings)
    }
    instance
  }
}
