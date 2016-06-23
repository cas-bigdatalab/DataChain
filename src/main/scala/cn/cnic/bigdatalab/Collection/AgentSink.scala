package cn.cnic.bigdatalab.collection

/**
  * Created by xjzhu on 16/6/22.
  */

class AgentSink(sinkName:String, sinkParameters:Map[String,String]){
  def getName(): String ={
    sinkName
  }

  def getParameters(): Map[String, String] ={
    sinkParameters
  }
}

