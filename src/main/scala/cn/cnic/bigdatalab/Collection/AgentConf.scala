package cn.cnic.bigdatalab.Collection

import org.apache.flume.Context
import org.apache.flume.{Source,Sink}
import org.apache.flume.source.{AvroSource, DefaultSourceFactory}

/**
  * Created by cnic on 2016/6/21.
  */
class AgentConf {
  var source:Source = _
  var sink:Sink = _


  def setSource(sourceName:String, sourceType:String): Unit ={
    val defaultSourceFactory:DefaultSourceFactory = new DefaultSourceFactory();
    source = defaultSourceFactory.create(sourceName, sourceType)
  }


}
