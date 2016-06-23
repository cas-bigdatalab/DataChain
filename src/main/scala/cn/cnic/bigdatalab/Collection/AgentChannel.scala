package cn.cnic.bigdatalab.collection

/**
  * Created by xjzhu on 16/6/23.
  */
class AgentChannel(name:String, channelParameters:Map[String,String]) {

  def getName(): String ={
    name
  }

  def getParameters():Map[String, String]={
    channelParameters
  }

}
