package cn.cnic.bigdatalab.collection

import org.apache.flume.source.DefaultSourceFactory
import org.apache.flume.{Context, Source}

/**
  * Created by xjzhu on 16/6/22.
  */



class AgentSource(sourceName:String, parameters:Map[String,String]){

  def getName(): String ={
    sourceName
  }

  def getParameters(): Map[String, String] ={
    parameters
  }

}

